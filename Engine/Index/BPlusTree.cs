using DB.Engine.Storage;


namespace DB.Engine.Index
{
    /// <summary>
    /// Minimal B+Tree skeleton (in-memory).
    /// This file implements:
    /// - constructor
    /// - FindLeaf (walks from root to the leaf that should contain a key)
    /// - small debug helper DumpLeafForKey(...) to inspect the leaf
    /// 
    /// will add Insert/Search/Range/Delete below this class.
    /// </summary>
    public class BPlusTree
    {
        /// <summary>
        /// Order = maximum number of children in an internal node.
        /// Must be >= 3. (Internal node max keys = Order - 1)
        /// </summary>
        public int Order { get; }

        // Root node reference (either InternalNode or LeafNode)
        private Node _root;

        public BPlusTree(int order)
        {
            if (order < 3)
                throw new ArgumentException("B+Tree order must be at least 3.", nameof(order));
            Order = order;
            _root = new LeafNode();
        }


        /// <summary>
        /// Public helper for tests: returns a human-readable representation
        /// of the keys present in the leaf that would contain rawKey.
        /// Useful to check FindLeaf navigation.
        /// </summary>
        public string DumpLeafForKey(Key rawKey)
        {
            var key = Key.FromObject(rawKey);
            var leaf = FindLeaf(key);
            if (leaf.Keys.Count == 0) return "(empty leaf)";
            var parts = new List<string>();
            for (int i = 0; i < leaf.Keys.Count; i++)
            {
                parts.Add(leaf.Keys[i].ToString());
            }
            return string.Join(", ", parts);
        }

        /// <summary>
        /// Find the leaf node that should contain the supplied key.
        /// Does not modify the tree.
        /// </summary>
        private LeafNode FindLeaf(Key key)
        {
            Node node = _root;

            while (!node.IsLeaf)
            {
                var inode = (InternalNode)node;

                int childIndex = LocateChildIndex(inode.Keys, key);
                node = inode.Children[childIndex];
            }

            return (LeafNode)node;
        }


        /// <summary>
        /// Locate the child index within an internal node for a given key.
        /// Returns an index in range [0 .. keys.Count] representing children[index].
        /// Behavior: if key equals a separator key, we choose the right-side child (index = keyIndex + 1).
        /// </summary>
        private static int LocateChildIndex(List<Key> Keys, Key key)
        {
            // Binary search: find first key >= target; child index = position where we'd descend
            int lo = 0, hi = Keys.Count - 1;

            while (lo <= hi)
            {
                int mid = (lo + hi) >> 1;
                int cmp = key.CompareTo(Keys[mid]);
                if (cmp < 0) hi = mid - 1;
                else if (cmp > 0) lo = mid + 1;
                else
                {
                    // Equal: go to right child
                    return mid + 1;
                }
            }
            return lo;
        }

        public void Insert(object rawKey, RID rid)
        {
            var key = Key.FromObject(rawKey);
            var leaf = FindLeaf(key);

            InsertIntoLeaf(leaf, key, rid);

            if (leaf.Keys.Count >= Order)
            {
                SplitLeaf(leaf);
            }
        }

        private void InsertIntoLeaf(LeafNode leaf, Key key, RID rid)
        {
            // Binary search to find insert position 
            int lo = 0, hi = leaf.Keys.Count - 1;

            while (lo <= hi)
            {
                int mid = (lo + hi) >> 1;
                int cmp = key.CompareTo(leaf.Keys[mid]);

                if (cmp < 0) hi = mid - 1;
                else if (cmp > 0) lo = mid + 1;
                else
                {
                    // Key already exists -> append RID
                    leaf.Values[mid].Add(rid);
                    return;
                }
            }

            // Key does not exist -> insert new key and RID list
            int insertIndex = lo;
            leaf.Keys.Insert(insertIndex, key);
            leaf.Values.Insert(insertIndex, [rid]);
        }

        private void SplitLeaf(LeafNode leaf)
        {
            int mid = leaf.Keys.Count / 2;

            var right = new LeafNode();

            // Move half of the keys and values to the new right leaf
            for (int i = mid; i < leaf.Keys.Count; i++)
            {
                right.Keys.Add(leaf.Keys[i]);
                right.Values.Add(leaf.Values[i]);
            }

            // Remove moved keys and values from the original leaf
            int removeCount = leaf.Keys.Count - mid;
            leaf.Keys.RemoveRange(mid, removeCount);
            leaf.Values.RemoveRange(mid, removeCount);

            // Fix linked list pointers
            right.Next = leaf.Next;
            if (right.Next != null)
            {
                right.Next.Prev = right;
            }

            leaf.Next = right;
            right.Prev = leaf;

            // Parent Handling 
            if (leaf.Parent == null)
            {
                var newRoot = new InternalNode();

                newRoot.Keys.Add(right.Keys[0]);
                newRoot.Children.Add(leaf);
                newRoot.Children.Add(right);

                leaf.Parent = newRoot;
                right.Parent = newRoot;

                _root = newRoot;
            }
            else
            {
                InsertIntoInternal((InternalNode)leaf.Parent, right.Keys[0], right);
            }
        }

        private void InsertIntoInternal(InternalNode parent, Key key, Node rightChild)
        {
            // find position to insert key

            int lo = 0, hi = parent.Keys.Count - 1;

            while (lo <= hi)
            {
                int mid = (lo + hi) >> 1;
                int cmp = key.CompareTo(parent.Keys[mid]);

                if (cmp < 0) hi = mid - 1;
                else
                {
                    lo = mid + 1;
                }
            }

            int insertIndex = lo;

            parent.Keys.Insert(insertIndex, key);
            parent.Children.Insert(insertIndex + 1, rightChild);
            rightChild.Parent = parent;

            if (parent.Children.Count >= Order)
            {
                SplitInternal(parent);
            }
        }


        private void SplitInternal(InternalNode node)
        {
            int midKeyIndex = node.Keys.Count / 2;
            Key promoteKey = node.Keys[midKeyIndex];

            var right = new InternalNode();

            // Move half of the keys and children to the new right internal node
            for (int i = midKeyIndex + 1; i < node.Keys.Count; i++)
            {
                right.Keys.Add(node.Keys[i]);
            }

            for (int i = midKeyIndex + 1; i < node.Children.Count; i++)
            {
                right.Children.Add(node.Children[i]);
                node.Children[i].Parent = right;
            }

            // Remove moved keys and children from the original node
            int removeKeys = node.Keys.Count - midKeyIndex;
            node.Keys.RemoveRange(midKeyIndex, removeKeys);

            int removeChildren = node.Children.Count - (midKeyIndex + 1);
            node.Children.RemoveRange(midKeyIndex + 1, removeChildren);

            right.Parent = node.Parent;

            if (node.Parent == null)
            {
                var newRoot = new InternalNode();
                newRoot.Keys.Add(promoteKey);
                newRoot.Children.Add(node);
                newRoot.Children.Add(right);

                node.Parent = newRoot;
                right.Parent = newRoot;
                _root = newRoot;
            } else
            {
                InsertIntoInternal((InternalNode)node.Parent, promoteKey, right);
            }
        }

        public string Dump()
        {
            var sb = new System.Text.StringBuilder();
            DumpNode(_root, 0, sb);
            return sb.ToString();
        }

        private void DumpNode(Node node, int level, System.Text.StringBuilder sb)
        {
            sb.Append(new string(' ', level * 2));

            if (node.IsLeaf)
            {
                var leaf = (LeafNode)node;
                sb.Append("Leaf: ");

                for (int i = 0; i < leaf.Keys.Count; i++)
                {
                    sb.Append($"[{leaf.Keys[i]} -> ");
                    sb.Append(string.Join(",", leaf.Values[i]));
                    sb.Append("] ");
                }

                sb.AppendLine();
            }
            else
            {
                var internalNode = (InternalNode)node;
                sb.Append("Internal: ");

                foreach (var key in internalNode.Keys)
                    sb.Append($"{key} ");

                sb.AppendLine();

                foreach (var child in internalNode.Children)
                    DumpNode(child, level + 1, sb);
            }
        }
        public IEnumerable<RID> Search(object rawKey)
        {
            var key = Key.FromObject(rawKey);
            var leaf = FindLeaf(key);

            int lo = 0, hi = leaf.Keys.Count - 1;

            while (lo <= hi)
            {
                int mid = (lo + hi) >> 1;
                int cmp = key.CompareTo(leaf.Keys[mid]);

                if (cmp < 0)
                    hi = mid - 1;
                else if (cmp > 0)
                    lo = mid + 1;
                else
                {
                    var list = leaf.Values[mid];
                    // enforce invariant: no empty RID list
                    return list.Count == 0
                        ? Enumerable.Empty<RID>()
                        : list;
                }
            }

            return Enumerable.Empty<RID>(); // NOT FOUND
        }

        // TODO: borrow/merge logic later
        public void Delete(object rawKey, RID rid)
        {
            var key = Key.FromObject(rawKey);
            var leaf = FindLeaf(key);

            int lo = 0, hi = leaf.Keys.Count - 1;

            while (lo <= hi)
            {
                int mid = (lo + hi) >> 1;
                int cmp = key.CompareTo(leaf.Keys[mid]);

                if (cmp < 0)
                    hi = mid - 1;
                else if (cmp > 0)
                    lo = mid + 1;
                else
                {
                    // key found → remove RID
                    var list = leaf.Values[mid];
                    list.RemoveAll(r => r.PageId == rid.PageId && r.SlotId == rid.SlotId);

                    // if no RIDs left, remove key
                    if (list.Count == 0)
                    {
                        leaf.Keys.RemoveAt(mid);
                        leaf.Values.RemoveAt(mid);
                    }
                    return;
                }
            }
        }

        public IEnumerable<(object Key, RID Rid)> RangeScan(object minRaw, object maxRaw)
        {
            var minKey = Key.FromObject(minRaw);
            var maxKey = Key.FromObject(maxRaw);

            // 1. Find the leaf where minKey should exist
            var leaf = FindLeaf(minKey);

            // 2. Find starting position inside that leaf
            int startIndex = 0;
            while (startIndex < leaf.Keys.Count &&
                   leaf.Keys[startIndex].CompareTo(minKey) < 0)
            {
                startIndex++;
            }

            // 3. Iterate leaf-by-leaf
            var current = leaf;
            int index = startIndex;

            while (current != null)
            {
                while (index < current.Keys.Count)
                {
                    var key = current.Keys[index];

                    // Stop when key > max
                    if (key.CompareTo(maxKey) > 0)
                        yield break;

                    foreach (var rid in current.Values[index])
                        yield return (key.Value, rid);

                    index++;
                }

                // move to next leaf
                current = current.Next;
                index = 0;
            }
        }


    }
}
