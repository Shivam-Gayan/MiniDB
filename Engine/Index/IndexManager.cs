using DB.Engine.Database;
using DB.Engine.Indexing;
using DB.Engine.Storage;
using System;
using System.Buffers.Binary;
using System.Collections.Generic;

namespace DB.Engine.Index
{
    /// <summary>
    /// IndexManager owns and manages all persistent indexes.
    /// Maps (table, column) -> disk-backed BPlusTree.
    /// </summary>
    public sealed class IndexManager
    {
        private readonly TableManager _tableManager;
        private readonly DatabaseContext _context;
        // table -> column -> tree
        private readonly Dictionary<string, Dictionary<string, BPlusTree>> _indexes
            = new(StringComparer.OrdinalIgnoreCase);

        public IndexManager(DatabaseContext context)
        {
            _context = context;
            _tableManager = context.TableManager;
            LoadAllIndexes();
        }

        private static string Normalize(string name) => name.ToLowerInvariant();

        // ------------------------------------------------------------
        // STARTUP: Load all indexes from disk metadata
        // ------------------------------------------------------------
        private void LoadAllIndexes()
        {
            foreach (var table in _tableManager.GetTableNames())
            {
                var handle = _context.GetIndexFileHandle(table);
                var fm = handle.FileManager;

                // ALWAYS read metadata head from index file header (page 0)
                var headerPage = new Page();
                headerPage.Load(fm.ReadPage(0));
                var header = IndexFileHeader.ReadFrom(headerPage.Buffer);

                int metaPid = header.FirstMetaPageId;
                handle.FirstMetaPageId = metaPid; // keep in sync

                if (metaPid < 0)
                    continue; // no indexes for this table

                while (metaPid >= 0)
                {
                    var metaPage = new Page();
                    metaPage.Load(fm.ReadPage(metaPid));

                    var meta = IndexMetadata.ReadFrom(metaPage.Buffer);

                    var tree = new BPlusTree(
                        fm,
                        meta.RootPageId,
                        meta.Order);

                    if (!_indexes.TryGetValue(table, out var map))
                    {
                        map = new Dictionary<string, BPlusTree>(StringComparer.OrdinalIgnoreCase);
                        _indexes[table] = map;
                    }

                    map[meta.ColumnName] = tree;

                    metaPid = meta.NextMetaPageId;
                }
            }
        }



        public void DropIndex(string tableName, string columnName)
        {
            tableName = Normalize(tableName);
            columnName = Normalize(columnName);

            if (!_indexes.TryGetValue(tableName, out var map) ||
                !map.ContainsKey(columnName))
            {
                throw new InvalidOperationException(
                    $"Index on {tableName}.{columnName} does not exist.");
            }

            // Remove from memory
            map.Remove(columnName);

            // Remove metadata from index file
            RemoveIndexMetadata(tableName, columnName);
        }
        private void RemoveIndexMetadata(string tableName, string columnName)
        {
            var handle = _context.GetIndexFileHandle(tableName);
            var fm = handle.FileManager;

            int prevPid = -1;
            int pid = handle.FirstMetaPageId;

            while (pid != -1)
            {
                var page = new Page();
                page.Load(fm.ReadPage(pid));

                var meta = IndexMetadata.ReadFrom(page.Buffer);

                if (meta.ColumnName.Equals(columnName, StringComparison.OrdinalIgnoreCase))
                {
                    if (prevPid < 0)
                    {
                        handle.FirstMetaPageId = meta.NextMetaPageId;
                    }
                    else
                    {
                        var prev = new Page();
                        prev.Load(fm.ReadPage(prevPid));
                        var prevMeta = IndexMetadata.ReadFrom(prev.Buffer);
                        prevMeta.NextMetaPageId = meta.NextMetaPageId;
                        prevMeta.WriteTo(prev.Buffer);
                        fm.WritePage(prevPid, prev.Buffer);
                    }

                    return;
                }

                prevPid = pid;
                pid = meta.NextMetaPageId;
            }
        }

        // ------------------------------------------------------------
        // CREATE INDEX (DDL)
        // ------------------------------------------------------------
        public void CreateIndex(string tableName, string columnName)
        {
            tableName = Normalize(tableName);
            columnName = Normalize(columnName);

            if (!_indexes.TryGetValue(tableName, out var map))
                _indexes[tableName] = map = new(StringComparer.OrdinalIgnoreCase);

            if (map.ContainsKey(columnName))
                throw new InvalidOperationException(
                    $"Index on {tableName}.{columnName} already exists.");

            var handle = _context.GetIndexFileHandle(tableName)
                ?? throw new InvalidOperationException("Index file handle missing.");

            var fm = handle.FileManager;

            // Allocate root leaf
            int rootPid = fm.AllocatePage(PageType.Index);
            var root = new Page();
            root.Load(fm.ReadPage(rootPid));

            IndexNodeHeader.Write(
                root.Buffer,
                isLeaf: true,
                keyCount: 0,
                parentPageId: -1
            );

            // If we leave these as 0, the scanner thinks the next page is Page 0 (Header),
            // causing the "Leaf Pointer is 0" corruption error.
            int payloadOffset = DbOptions.HeaderSize + IndexNodeHeader.Size;

            // Prev Pointer (-1)
            BinaryPrimitives.WriteInt32LittleEndian(
                root.Buffer.AsSpan(payloadOffset), -1);

            // Next Pointer (-1)
            BinaryPrimitives.WriteInt32LittleEndian(
                root.Buffer.AsSpan(payloadOffset + 4), -1);
            // ---------------------------------------------------------

            fm.WritePage(rootPid, root.Buffer);

            // Write metadata page
            var meta = new IndexMetadata
            {
                ColumnName = columnName,
                RootPageId = rootPid,
                Order = DbOptions.DefaultIndexOrder,
                NextMetaPageId = handle.FirstMetaPageId
            };

            int metaPid = fm.AllocatePage(PageType.Index);
            var metaPage = new Page();
            metaPage.Load(fm.ReadPage(metaPid));
            meta.WriteTo(metaPage.Buffer);
            fm.WritePage(metaPid, metaPage.Buffer);

            handle.FirstMetaPageId = metaPid;
            var headerPage = new Page();
            headerPage.Load(fm.ReadPage(0));

            var header = IndexFileHeader.ReadFrom(headerPage.Buffer);
            header.FirstMetaPageId = metaPid;
            header.IndexCount++;

            header.WriteTo(headerPage.Buffer);
            fm.WritePage(0, headerPage.Buffer);

            map[columnName] = new BPlusTree(fm, rootPid, DbOptions.DefaultIndexOrder);
        }

        public void OnInsert(string table, Record record, RID rid)
        {
            table = Normalize(table);

            if (!_indexes.TryGetValue(table, out var map))
                return; // no indexes on this table

            for (int i = 0; i < record.Schema.Columns.Count; i++)
            {
                string col = Normalize(record.Schema.Columns[i]);

                if (!map.TryGetValue(col, out var tree))
                    continue;

                var value = record.Values[i];
                if (value == null)
                    continue;

                var key = Key.FromObject(value);
                tree.Insert(key, rid);
            }
        }

        // ------------------------------------------------------------
        // ROUTING OPERATIONS
        // ------------------------------------------------------------
        public bool HasIndex(string table, string column)
        {
            table = Normalize(table);
            column = Normalize(column);
            return _indexes.TryGetValue(table, out var map) && map.ContainsKey(column);
        }

        public void Insert(string table, string column, object key, RID rid)
        {
            if (TryGetTree(table, column, out var tree))
                try
                {
                    tree.Insert(key, rid);
                }
                catch (InvalidOperationException ex)
                {
                    throw new InvalidOperationException(
                        $"UNIQUE index violation on {table}.{column}: {ex.Message}");
                }

        }

        public void ValidateUnique(string table, string column, object key)
        {
            if (TryGetTree(table, column, out var tree))
            {
                if (tree.Contains(key))
                {
                    throw new InvalidOperationException(
                        $"UNIQUE index violation on {table}({column}) for value '{key}'.");
                }
            }
        }

        public void Delete(string table, string column, object key, RID rid)
        {
            if (TryGetTree(table, column, out var tree))
                tree.Delete(key, rid);
        }
        public bool Exists(string table, string column, object key)
        {
            if (!TryGetTree(table, column, out var tree))
                return false;

            // Search returns IEnumerable<RID>
            return tree.Search(key).Any();
        }


        public IEnumerable<RID> Search(string table, string column, object key)
        {
            if (!TryGetTree(table, column, out var tree))
                throw new InvalidOperationException($"No index on {table}.{column}");

            return tree.Search(key);
        }

        public IEnumerable<(object Key, RID Rid)> RangeScan(string table, string column, object? min, object? max)
        {
            if (!TryGetTree(table, column, out var tree))
                yield break;

            Key? minKey = min != null ? Key.FromObject(min) : null;
            Key? maxKey = max != null ? Key.FromObject(max) : null;

            foreach (var kv in tree.RangeScan(minKey, maxKey))
                yield return kv;
        }


        // ------------------------------------------------------------
        // HELPERS
        // ------------------------------------------------------------
        private bool TryGetTree(string table, string column, out BPlusTree tree)
        {
            table = Normalize(table);
            column = Normalize(column);

            tree = null!;
            return _indexes.TryGetValue(table, out var map)
                && map.TryGetValue(column, out tree);
        }

        public IReadOnlyList<(string Table, string Column)> ListIndexes()
        {
            var result = new List<(string, string)>();

            foreach (var (table, map) in _indexes)
            {
                foreach (var column in map.Keys)
                    result.Add((table, column));
            }

            return result;
        }

        public void DropIndexesForTable(string table)
        {
            table = Normalize(table);
            _indexes.Remove(table);
        }
    }
}
