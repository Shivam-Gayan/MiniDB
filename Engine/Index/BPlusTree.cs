using System.Buffers.Binary;
using System.Text;
using DB.Engine.Storage;
using DB.Engine.Indexing;

namespace DB.Engine.Index
{
    public sealed class BPlusTree
    {
        private readonly FileManager _fm;
        private readonly int _order;

        public int RootPageId { get; private set; }

        private const int PayloadOffset =
            DbOptions.HeaderSize + IndexNodeHeader.Size;

        public BPlusTree(FileManager fm, int rootPageId, int order)
        {
            _fm = fm;
            _order = order;
            RootPageId = rootPageId;
            ValidateRoot();
        }

        // ---------------- PAGE HELPERS ----------------
        private void ValidateRoot()
        {
            var root = Load(RootPageId);

            IndexNodeHeader.Read(
                root.Buffer,
                out bool isLeaf,
                out short keyCount,
                out int parentPid
            );

            if (keyCount < 0)
                throw new InvalidOperationException("Corrupted index root");

            if (parentPid != -1)
                throw new InvalidOperationException("Root node has a parent pointer");
        }


        private Page Load(int pageId)
        {
            var p = new Page();
            p.Load(_fm.ReadPage(pageId));
            return p;
        }

        private void Flush(Page p)
        {
            _fm.WritePage(p.PageId, p.Buffer);
        }

        private int AllocateNode(bool isLeaf, int parentPid)
        {
            int pid = _fm.AllocatePage(PageType.Index);
            var p = Load(pid);

            IndexNodeHeader.Write(
                p.Buffer,
                isLeaf,
                keyCount: 0,
                parentPageId: parentPid
            );

            // leaf links init
            if (isLeaf)
            {
                BinaryPrimitives.WriteInt32LittleEndian(
                    p.Buffer.AsSpan(PayloadOffset), -1);
                BinaryPrimitives.WriteInt32LittleEndian(
                    p.Buffer.AsSpan(PayloadOffset + 4), -1);
            }

            Flush(p);
            return pid;
        }

        // ---------------- KEY CODEC ----------------

        private void WriteKey(byte[] buffer, ref int offset, Key key)
        {
            buffer[offset++] = (byte)key.Type;

            switch (key.Type)
            {
                case FieldType.Integer:
                    BinaryPrimitives.WriteInt32LittleEndian(
                        buffer.AsSpan(offset), (int)key.Value);
                    offset += 4;
                    break;

                case FieldType.Double:
                    BinaryPrimitives.WriteDoubleLittleEndian(
                        buffer.AsSpan(offset), (double)key.Value);
                    offset += 8;
                    break;

                case FieldType.Boolean:
                    buffer[offset++] = (byte)((bool)key.Value ? 1 : 0);
                    break;

                case FieldType.String:
                    var bytes = Encoding.UTF8.GetBytes((string)key.Value);
                    BinaryPrimitives.WriteInt16LittleEndian(
                        buffer.AsSpan(offset), (short)bytes.Length);
                    offset += 2;
                    bytes.CopyTo(buffer.AsSpan(offset));
                    offset += bytes.Length;
                    break;

                default:
                    throw new InvalidOperationException($"Unsupported key type {key.Type}");
            }
        }


        private static Key ReadKey(ReadOnlySpan<byte> buf, ref int off)
        {
            var type = (FieldType)buf[off++];

            switch (type)
            {
                case FieldType.Integer:
                    var iVal = BinaryPrimitives.ReadInt32LittleEndian(buf.Slice(off, 4));
                    off += 4;
                    return new Key(type, iVal);

                case FieldType.Double:
                    var dVal = BinaryPrimitives.ReadDoubleLittleEndian(buf.Slice(off, 8));
                    off += 8; 
                    return new Key(type, dVal);

                case FieldType.Boolean:
                    return new Key(type, buf[off++] == 1);

                case FieldType.String:
                    return ReadStringKey(buf, ref off);

                default:
                    throw new InvalidOperationException($"Unknown Key Type: {type}");
            }
        }

        private static Key ReadStringKey(ReadOnlySpan<byte> buf, ref int off)
        {
            ushort len = BinaryPrimitives.ReadUInt16LittleEndian(buf.Slice(off, 2));
            off += 2;
            var s = Encoding.UTF8.GetString(buf.Slice(off, len));
            off += len;
            return new Key(FieldType.String, s);
        }

        // ---------------- TREE NAVIGATION ----------------

        private int FindLeafPage(Key key)
        {
            int current = RootPageId;

            while (true)
            {
                var p = Load(current);
                IndexNodeHeader.Read(p.Buffer, out bool isLeaf, out short keyCount, out _);

                if (isLeaf)
                    return current;

                current = ChooseChild(p, key, keyCount);
            }
        }

        private int ChooseChild(Page p, Key key, short keyCount)
        {
            int off = PayloadOffset;

            for (int i = 0; i < keyCount; i++)
            {
                // 1. Read the Child Pointer
                int childPid = BinaryPrimitives.ReadInt32LittleEndian(p.Buffer.AsSpan(off));
                off += 4;

                // 2. Read the Key
                var k = ReadKey(p.Buffer, ref off);

                // 3. Comparison
                if (key.CompareTo(k) < 0)
                    return childPid;
            }

            // 4. The Right-Most Child is stored at the very END
            return BinaryPrimitives.ReadInt32LittleEndian(p.Buffer.AsSpan(off));
        }

        // ---------------- INSERT ----------------

        public void Insert(object rawKey, RID rid)
        {
            var key = Key.FromObject(rawKey);
            int leafPid = FindLeafPage(key);

            InsertIntoLeaf(leafPid, key, rid);
        }

        private void InsertIntoLeaf(int leafPid, Key key, RID rid)
        {
            var p = Load(leafPid);
            IndexNodeHeader.Read(p.Buffer, out _, out short keyCount, out int parentPid);

            // Read existing entries
            var entries = ReadLeafEntries(p, keyCount);

            InsertEntry(entries, key, rid);

            if (entries.Count < _order)
            {
                WriteLeaf(p, parentPid, entries);
                Flush(p);
                return;
            }

            SplitLeaf(p, entries, parentPid);
        }

        // ---------------- LEAF SPLIT ----------------

        private void SplitLeaf(Page leaf, List<(Key key, List<RID> rids)> entries, int parentPid)
        {
            int mid = entries.Count / 2;

            var rightEntries = entries.GetRange(mid, entries.Count - mid);
            entries.RemoveRange(mid, entries.Count - mid);

            int rightPid = AllocateNode(isLeaf: true, parentPid);
            var right = Load(rightPid);

            // Write both leaves
            WriteLeaf(leaf, parentPid, entries);
            WriteLeaf(right, parentPid, rightEntries);

            // Maintain leaf linked list
            int oldNext = BinaryPrimitives.ReadInt32LittleEndian(
                leaf.Buffer.AsSpan(PayloadOffset + 4));

            BinaryPrimitives.WriteInt32LittleEndian(
                leaf.Buffer.AsSpan(PayloadOffset + 4), rightPid);

            BinaryPrimitives.WriteInt32LittleEndian(
                right.Buffer.AsSpan(PayloadOffset), leaf.PageId);

            BinaryPrimitives.WriteInt32LittleEndian(
                right.Buffer.AsSpan(PayloadOffset + 4), oldNext);

            Flush(leaf);
            Flush(right);

            if (oldNext != -1)
            {
                var nextNode = Load(oldNext);
                // Update the PREV pointer (offset 0 in payload) of the neighbor
                BinaryPrimitives.WriteInt32LittleEndian(
                    nextNode.Buffer.AsSpan(PayloadOffset),
                    rightPid
                );
                Flush(nextNode);
            }

            // promote FIRST key of right node
            InsertIntoParent(leaf.PageId, rightEntries[0].key, rightPid);
        }

        // ---------------- INTERNAL INSERT ----------------

        private void InsertIntoParent(int leftPid, Key key, int rightPid)
        {
            var left = Load(leftPid);
            IndexNodeHeader.Read(left.Buffer, out _, out _, out int parentPid);

            if (parentPid == -1)
            {
                int newRootPid = AllocateNode(isLeaf: false, -1);
                var root = Load(newRootPid);

                WriteInternal(
                    root,
                    -1,
                    new List<(Key, int)> { (key, leftPid) },
                    rightPid
                );

                // update child parents
                SetParent(leftPid, newRootPid);
                SetParent(rightPid, newRootPid);

                RootPageId = newRootPid;
                Flush(root);
                return;
            }

            var parent = Load(parentPid);
            InsertIntoInternal(parent, key, rightPid);
        }

        private void WriteInternal(Page page, int parentPid, List<(Key key, int childPid)> entries, int rightMostChildPid)
        {
            IndexNodeHeader.Write(
                page.Buffer,
                isLeaf: false,
                keyCount: (short)entries.Count,
                parentPageId: parentPid
            );

            int off = PayloadOffset;

            // Write children + keys
            foreach (var (key, childPid) in entries)
            {
                BinaryPrimitives.WriteInt32LittleEndian(
                    page.Buffer.AsSpan(off), childPid);
                off += 4;

                WriteKey(page.Buffer, ref off, key);
            }

            // write RIGHTMOST child pointer
            BinaryPrimitives.WriteInt32LittleEndian(
                page.Buffer.AsSpan(off), rightMostChildPid);

            Flush(page);
        }

        private void InsertIntoInternal(Page parent, Key key, int rightChildPid)
        {
            IndexNodeHeader.Read(parent.Buffer, out _, out short keyCount, out int parentPid);

            var (entries, rightMostChildPid) = ReadInternal(parent, keyCount);

            // Find insert position
            int i = 0;
            while (i < entries.Count && entries[i].key.CompareTo(key) < 0)
                i++;

            // logic to handle the shift correctly
            if (i == entries.Count)
            {
                // Inserting at the end: Old RightMost becomes Left Child of new Key.
                // New Child becomes the new RightMost.
                entries.Add((key, rightMostChildPid));
                rightMostChildPid = rightChildPid;
            }
            else
            {
                // Inserting in middle: We split the child at 'i'.
                // The existing child at entries[i].childPid (Left of old Key[i]) 
                // becomes the Left Child of our NEW Key.
                int leftPid = entries[i].childPid;

                // Insert new key pointing to the existing left node
                entries.Insert(i, (key, leftPid));

                // The NEXT entry (which was the old entries[i]) must now point 
                // to our new rightChildPid as its Left Child.
                var nextEntry = entries[i + 1];
                entries[i + 1] = (nextEntry.key, rightChildPid);
            }

            // ... rest of the function (overflow check) remains the same
            if (entries.Count < _order)
            {
                WriteInternal(parent, parentPid, entries, rightMostChildPid);
                Flush(parent);
                return;
            }

            SplitInternal(parent, entries, rightMostChildPid, parentPid);
        }

        private (List<(Key key, int childPid)> entries, int rightMostChildPid) ReadInternal(Page page, int keyCount)
        {
            var entries = new List<(Key, int)>(keyCount);

            int off = PayloadOffset;

            for (int i = 0; i < keyCount; i++)
            {
                // READ child pointer FIRST
                int childPid = BinaryPrimitives.ReadInt32LittleEndian(
                    page.Buffer.AsSpan(off));
                off += 4;

                // THEN read key
                var key = ReadKey(page.Buffer, ref off);

                entries.Add((key, childPid));
            }

            // READ right-most child
            int rightMostChildPid = BinaryPrimitives.ReadInt32LittleEndian(
                page.Buffer.AsSpan(off));

            return (entries, rightMostChildPid);
        }

        private void SplitInternal(Page left, List<(Key key, int childPid)> entries, int rightMostChildPid, int parentPid)
        {
            int mid = entries.Count / 2;
            var promoteKey = entries[mid].key;

            // SAVE the child pointer to the left of the promoted key.
            // This node becomes the RightMost child of the LEFT page.
            int leftRightMost = entries[mid].childPid;

            var rightEntries = entries.GetRange(mid + 1, entries.Count - (mid + 1));
            entries.RemoveRange(mid, entries.Count - mid);

            int rightPid = AllocateNode(isLeaf: false, parentPid);
            var right = Load(rightPid);

            // Use 'leftRightMost' instead of 'entries.Last().childPid'
            WriteInternal(left, parentPid, entries, leftRightMost);
            WriteInternal(right, parentPid, rightEntries, rightMostChildPid);

            // Update parents for all children moved to the new right node
            UpdateChildrenParent(rightEntries, rightMostChildPid, rightPid);

            Flush(left);
            Flush(right);

            InsertIntoParent(left.PageId, promoteKey, rightPid);
        }

        // ---------------- BORROW / MERGE (CORE IDEA) ----------------

        private void HandleUnderflow(Page node)
        {
            IndexNodeHeader.Read(node.Buffer, out bool isLeaf, out _, out _);

            if (isLeaf)
            {
                if (BorrowFromLeftLeaf(node)) return;
                if (BorrowFromRightLeaf(node)) return;
                MergeLeafNode(node);
            }
            else
            {
                if (BorrowFromLeftInternal(node)) return;
                if (BorrowFromRightInternal(node)) return;
                MergeInternal(node);
            }

            TryShrinkRoot();
        }
        private bool BorrowFromRightLeaf(Page leaf)
        {
            var (leftPid, parentPid, sepIndex) =
                FindLeftSibling(leaf.PageId);

            // actually find RIGHT sibling
            var parent = Load(parentPid);
            var (entries, rightMost) =
                ReadInternal(parent, GetKeyCount(parent));

            var children = new List<int>();
            foreach (var e in entries)
                children.Add(e.childPid);
            children.Add(rightMost);

            int index = children.IndexOf(leaf.PageId);
            if (index >= children.Count - 1)
                return false;

            int rightPid = children[index + 1];
            var right = Load(rightPid);

            var rightEntries = ReadLeafEntries(right, GetKeyCount(right));
            if (rightEntries.Count <= MinKeys(true))
                return false;

            var leafEntries = ReadLeafEntries(leaf, GetKeyCount(leaf));

            // borrow first entry from right
            var borrowed = rightEntries[0];
            rightEntries.RemoveAt(0);
            leafEntries.Add(borrowed);

            WriteLeaf(right, parentPid, rightEntries);
            WriteLeaf(leaf, parentPid, leafEntries);

            UpdateParentSeparator(
                parentPid,
                index,
                rightEntries[0].Item1);

            return true;
        }

        private (int leftPid, int parentPid, int sepIndex) FindLeftSibling(int leafPid)
        {
            var leaf = Load(leafPid);
            IndexNodeHeader.Read(leaf.Buffer, out _, out _, out int parentPid);
            if (parentPid < 0)
                return (-1, -1, -1);

            var parent = Load(parentPid);
            var (entries, rightMost) =
                ReadInternal(parent, GetKeyCount(parent));

            var children = new List<int>();
            foreach (var e in entries)
                children.Add(e.childPid);
            children.Add(rightMost);

            int index = children.IndexOf(leafPid);

            if (index <= 0)
                return (-1, parentPid, -1);

            return (children[index - 1], parentPid, index - 1);
        }

        private bool BorrowFromLeftLeaf(Page leaf)
        {
            var (leftPid, parentPid, sepIndex) = FindLeftSibling(leaf.PageId);
            if (leftPid < 0) return false;

            var left = Load(leftPid);

            var leftEntries = ReadLeafEntries(left, GetKeyCount(left));
            var leafEntries = ReadLeafEntries(leaf, GetKeyCount(leaf));

            if (leftEntries.Count <= MinKeys(true))
                return false;

            // move last entry
            var borrowed = leftEntries.Last();
            leftEntries.RemoveAt(leftEntries.Count - 1);
            leafEntries.Insert(0, borrowed);

            WriteLeaf(left, parentPid, leftEntries);
            WriteLeaf(leaf, parentPid, leafEntries);

            UpdateParentSeparator(parentPid, sepIndex, leafEntries[0].Item1);
            return true;
        }
        private void MergeLeaf(Page left, Page right, int parentPid)
        {
            var leftEntries = ReadLeafEntries(left, GetKeyCount(left));
            var rightEntries = ReadLeafEntries(right, GetKeyCount(right));

            leftEntries.AddRange(rightEntries);

            WriteLeaf(left, parentPid, leftEntries);
            MarkPageFree(right.PageId);

            RemoveParentSeparator(parentPid, right.PageId);
        }
        private void MergeLeafNode(Page leaf)
        {
            var (leftPid, parentPid, sepIndex) =
                FindLeftSibling(leaf.PageId);

            if (leftPid < 0)
                return;

            var left = Load(leftPid);

            MergeLeaf(left, leaf, parentPid);
        }
        private void UpdateParentSeparator(int parentPid,int sepIndex,Key newKey)
        {
            var parent = Load(parentPid);
            var (entries, rightMost) =
                ReadInternal(parent, GetKeyCount(parent));

            entries[sepIndex] =
                (newKey, entries[sepIndex].childPid);

            WriteInternal(parent, -1, entries, rightMost);
            Flush(parent);
        }

        private void RemoveParentSeparator(int parentPid,int removedChildPid)
        {
            var parent = Load(parentPid);
            var (entries, rightMost) =
                ReadInternal(parent, GetKeyCount(parent));

            for (int i = 0; i < entries.Count; i++)
            {
                if (entries[i].childPid == removedChildPid)
                {
                    entries.RemoveAt(i);
                    break;
                }
            }

            WriteInternal(parent, -1, entries, rightMost);
            Flush(parent);
        }
        private int ReadSingleChild(Page root)
        {
            int off = PayloadOffset;
            return BinaryPrimitives.ReadInt32LittleEndian(
                root.Buffer.AsSpan(off));
        }

        private void MarkPageFree(int pageId)
        {
            var p = Load(pageId);
            p.Header.PageType = PageType.Free;
            Flush(p);
        }

        private (int leftPid, int rightPid, int sepIndex, Page parent) FindInternalSiblings(Page node)
        {
            IndexNodeHeader.Read(node.Buffer, out _, out _, out int parentPid);
            if (parentPid < 0)
                return (-1, -1, -1, null!);

            var parent = Load(parentPid);
            var (entries, rightMost) =
                ReadInternal(parent, GetKeyCount(parent));

            // children array reconstruction
            var children = new List<int>();
            foreach (var e in entries)
                children.Add(e.childPid);
            children.Add(rightMost);

            int index = children.IndexOf(node.PageId);

            int leftPid = index > 0 ? children[index - 1] : -1;
            int rightPid = index < children.Count - 1 ? children[index + 1] : -1;

            int sepIndex = index - 1; // separator key index

            return (leftPid, rightPid, sepIndex, parent);
        }
        private bool BorrowFromLeftInternal(Page node)
        {
            var (leftPid, _, sepIndex, parent) = FindInternalSiblings(node);
            if (leftPid < 0) return false;

            var left = Load(leftPid);

            if (GetKeyCount(left) <= MinKeys(isLeaf: false))
                return false;

            // Read structures
            var (parentEntries, parentRight) =
                ReadInternal(parent, GetKeyCount(parent));
            var (leftEntries, leftRight) =
                ReadInternal(left, GetKeyCount(left));
            var (nodeEntries, nodeRight) =
                ReadInternal(node, GetKeyCount(node));

            // Rotation:
            // parentKey -> node
            // left maxKey -> parent
            var borrowedChild = leftRight;
            var borrowedKey = leftEntries.Last().key;

            leftRight = leftEntries.Last().childPid;
            leftEntries.RemoveAt(leftEntries.Count - 1);

            var parentKey = parentEntries[sepIndex].key;
            parentEntries[sepIndex] = (borrowedKey, parentEntries[sepIndex].childPid);

            nodeEntries.Insert(0, (parentKey, borrowedChild));

            // Persist
            WriteInternal(left, parent.PageId, leftEntries, leftRight);
            WriteInternal(node, parent.PageId, nodeEntries, nodeRight);
            WriteInternal(parent, -1, parentEntries, parentRight);

            Flush(left);
            Flush(node);
            Flush(parent);

            return true;
        }
        private bool BorrowFromRightInternal(Page node)
        {
            var (_, rightPid, sepIndex, parent) = FindInternalSiblings(node);
            if (rightPid < 0) return false;

            var right = Load(rightPid);

            if (GetKeyCount(right) <= MinKeys(isLeaf: false))
                return false;

            var (parentEntries, parentRight) =
                ReadInternal(parent, GetKeyCount(parent));
            var (rightEntries, rightRight) =
                ReadInternal(right, GetKeyCount(right));
            var (nodeEntries, nodeRight) =
                ReadInternal(node, GetKeyCount(node));

            // rotation
            var borrowedKey = rightEntries[0].key;
            var borrowedChild = rightEntries[0].childPid;

            rightEntries.RemoveAt(0);

            var parentKey = parentEntries[sepIndex].key;
            parentEntries[sepIndex] = (borrowedKey, parentEntries[sepIndex].childPid);

            nodeEntries.Add((parentKey, nodeRight));
            nodeRight = borrowedChild;

            WriteInternal(right, parent.PageId, rightEntries, rightRight);
            WriteInternal(node, parent.PageId, nodeEntries, nodeRight);
            WriteInternal(parent, -1, parentEntries, parentRight);

            Flush(right);
            Flush(node);
            Flush(parent);

            return true;
        }
        private void MergeInternal(Page node)
        {
            var (leftPid, rightPid, sepIndex, parent) =
                FindInternalSiblings(node);

            if (leftPid < 0 && rightPid < 0)
                return;

            Page left, right;

            if (leftPid >= 0)
            {
                left = Load(leftPid);
                right = node;
            }
            else
            {
                left = node;
                right = Load(rightPid);
            }

            var (parentEntries, parentRight) =
                ReadInternal(parent, GetKeyCount(parent));
            var sepKey = parentEntries[sepIndex].key;

            var (leftEntries, leftRight) =
                ReadInternal(left, GetKeyCount(left));
            var (rightEntries, rightRight) =
                ReadInternal(right, GetKeyCount(right));

            // Merge: left + separator + right
            leftEntries.Add((sepKey, leftRight));
            leftEntries.AddRange(rightEntries);
            leftRight = rightRight;

            parentEntries.RemoveAt(sepIndex);

            WriteInternal(left, parent.PageId, leftEntries, leftRight);
            WriteInternal(parent, -1, parentEntries, parentRight);

            Flush(left);
            Flush(parent);

            MarkPageFree(right.PageId);

            if (parentEntries.Count < MinKeys(isLeaf: false))
                HandleUnderflow(parent);
        }
        private void TryShrinkRoot()
        {
            var root = Load(RootPageId);
            IndexNodeHeader.Read(root.Buffer, out bool isLeaf, out short keyCount, out _);

            if (!isLeaf && keyCount == 0)
            {
                int newRootPid = ReadSingleChild(root);
                RootPageId = newRootPid;
                MarkPageFree(root.PageId);
            }
        }


        // ---------------- LOW-LEVEL READ / WRITE ----------------

        private List<(Key, List<RID>)> ReadLeafEntries(Page p, short keyCount)
        {
            var list = new List<(Key, List<RID>)>();
            int off = PayloadOffset + 8;

            for (int i = 0; i < keyCount; i++)
            {
                var key = ReadKey(p.Buffer, ref off);
                short rc = BinaryPrimitives.ReadInt16LittleEndian(p.Buffer.AsSpan(off));
                off += 2;

                var rids = new List<RID>();
                for (int r = 0; r < rc; r++)
                {
                    int pid = BinaryPrimitives.ReadInt32LittleEndian(p.Buffer.AsSpan(off));
                    int sid = BinaryPrimitives.ReadInt32LittleEndian(p.Buffer.AsSpan(off + 4));
                    off += 8;
                    rids.Add(new RID(pid, sid));
                }
                list.Add((key, rids));
            }
            return list;
        }

        private void WriteLeaf(Page p, int parentPid, List<(Key, List<RID>)> entries)
        {
            IndexNodeHeader.Write(p.Buffer, true, (short)entries.Count, parentPid);

            int off = PayloadOffset + 8;

            foreach (var (key, rids) in entries)
            {
                WriteKey(p.Buffer,ref off, key);
                BinaryPrimitives.WriteInt16LittleEndian(p.Buffer.AsSpan(off), (short)rids.Count);
                off += 2;

                foreach (var rid in rids)
                {
                    BinaryPrimitives.WriteInt32LittleEndian(p.Buffer.AsSpan(off), rid.PageId);
                    BinaryPrimitives.WriteInt32LittleEndian(p.Buffer.AsSpan(off + 4), rid.SlotId);
                    off += 8;
                }
            }
        }

        private void InsertEntry(List<(Key, List<RID>)> entries, Key key, RID rid)
        {
            int i = 0;
            while (i < entries.Count && entries[i].Item1.CompareTo(key) < 0)
                i++;

            // Block duplicate keys
            if (i < entries.Count && entries[i].Item1.CompareTo(key) == 0)
                throw new InvalidOperationException($"Duplicate key '{key.Value}'");

            // Insert new unique key
            entries.Insert(i, (key, new List<RID> { rid }));
        }


        // --- INTERNAL READ/WRITE OMITTED FOR BREVITY ---
        // Same pattern as leaf, but (Key, ChildPid) + RightMostChild


        public IEnumerable<RID> Search(object rawKey)
        {
            var key = Key.FromObject(rawKey);
            int leafPid = FindLeafPage(key);

            var leaf = Load(leafPid);
            IndexNodeHeader.Read(leaf.Buffer, out _, out short keyCount, out _);

            int off = PayloadOffset + 8; // skip prev/next

            for (int i = 0; i < keyCount; i++)
            {
                var k = ReadKey(leaf.Buffer, ref off);
                short rc = BinaryPrimitives.ReadInt16LittleEndian(
                    leaf.Buffer.AsSpan(off));
                off += 2;

                if (k.CompareTo(key) == 0)
                {
                    for (int r = 0; r < rc; r++)
                    {
                        int pid = BinaryPrimitives.ReadInt32LittleEndian(
                            leaf.Buffer.AsSpan(off));
                        int sid = BinaryPrimitives.ReadInt32LittleEndian(
                            leaf.Buffer.AsSpan(off + 4));
                        off += 8;
                        yield return new RID(pid, sid);
                    }
                    yield break;
                }

                off += rc * 8;
            }
        }
        public IEnumerable<(object Key, RID Rid)> RangeScan(Key? minKey, Key? maxKey)
        {
            int leafPid;

            if (minKey != null)
                leafPid = FindLeafPage(minKey);
            else
                leafPid = FindLeftMostLeaf();

            while (leafPid != -1)
            {
                var leaf = Load(leafPid);
                IndexNodeHeader.Read(leaf.Buffer, out _, out short keyCount, out _);

                int off = PayloadOffset;

                // Skip Prev pointer (4 bytes)
                // Read Next pointer (4 bytes) at offset + 4
                int next = BinaryPrimitives.ReadInt32LittleEndian(leaf.Buffer.AsSpan(off + 4));
                off += 8; // Move past Prev and Next pointers

                for (int i = 0; i < keyCount; i++)
                {
                    var key = ReadKey(leaf.Buffer, ref off);
                    short rc = BinaryPrimitives.ReadInt16LittleEndian(leaf.Buffer.AsSpan(off));
                    off += 2;

                    // Optimization: Stop early if we exceeded maxKey
                    if (maxKey != null && key.CompareTo(maxKey) > 0)
                        yield break;

                    if (minKey == null || key.CompareTo(minKey) >= 0)
                    {
                        for (int r = 0; r < rc; r++)
                        {
                            int pid = BinaryPrimitives.ReadInt32LittleEndian(leaf.Buffer.AsSpan(off));
                            int sid = BinaryPrimitives.ReadInt32LittleEndian(leaf.Buffer.AsSpan(off + 4));
                            yield return (key.Value, new RID(pid, sid));
                            off += 8;
                        }
                    }
                    else
                    {
                        // Key is smaller than minKey, skip its RIDs
                        off += rc * 8;
                    }
                }

                leafPid = next;
            }
        }

        private int FindLeftMostLeaf()
        {
            int pid = RootPageId;

            while (true)
            {
                var page = Load(pid);
                IndexNodeHeader.Read(page.Buffer, out bool isLeaf, out _, out _);
                if (isLeaf)
                    return pid;

                int off = PayloadOffset;
                pid = BinaryPrimitives.ReadInt32LittleEndian(
                    page.Buffer.AsSpan(off)); // first child
            }
        }

        public void Delete(object rawKey, RID rid)
        {
            var key = Key.FromObject(rawKey);
            int leafPid = FindLeafPage(key);

            var leaf = Load(leafPid);
            IndexNodeHeader.Read(leaf.Buffer, out _, out short keyCount, out int parentPid);

            var entries = ReadLeafEntries(leaf, keyCount);

            for (int i = 0; i < entries.Count; i++)
            {
                if (entries[i].Item1.CompareTo(key) == 0)
                {
                    entries[i].Item2.RemoveAll(
                        r => r.PageId == rid.PageId && r.SlotId == rid.SlotId);

                    if (entries[i].Item2.Count == 0)
                        entries.RemoveAt(i);

                    break;
                }
            }

            WriteLeaf(leaf, parentPid, entries);
            Flush(leaf);

            if (entries.Count < MinKeys(isLeaf: true))
                HandleUnderflow(leaf);
        }
        private short GetKeyCount(Page p)
        {
            IndexNodeHeader.Read(p.Buffer, out _, out short kc, out _);
            return kc;
        }

        private int MinKeys(bool isLeaf)
        {
            return isLeaf
                ? (_order - 1) / 2
                : (_order / 2) - 1;
        }
        public bool Contains(object rawKey)
        {
            var key = Key.FromObject(rawKey);
            return Search(key).Any();
        }

        private void SetParent(int childPid, int parentPid)
        {
            var c = Load(childPid);
            IndexNodeHeader.Read(c.Buffer, out bool isLeaf, out short kc, out _);
            IndexNodeHeader.Write(c.Buffer, isLeaf, kc, parentPid);
            Flush(c);
        }

        private void UpdateChildrenParent(List<(Key key, int childPid)> entries, int rightMostChildPid, int parentPid)
        {
            foreach (var (_, pid) in entries)
                SetParent(pid, parentPid);

            SetParent(rightMostChildPid, parentPid);
        }

    }
}
