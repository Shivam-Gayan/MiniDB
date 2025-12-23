using DB.Engine.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DB.Engine.Index
{
    /// <summary>
    /// IndexManager owns and manages all indexes in the database.
    /// It maps (table, column) -> BPlusTree.
    /// </summary>
    public sealed class IndexManager
    {
        // key format : " table.column"
        private readonly Dictionary<string, BPlusTree> _indexes = [];

        private readonly int _treeOrder;

        public IndexManager(int treeOrder)
        {
            if (treeOrder < 3)
            {
                throw new ArgumentException("B+ tree order must be at least 3.", nameof(treeOrder));
            }
            _treeOrder = treeOrder;
        }

        private static string MakeKey(string tableName, string columnName) => $" {tableName}.{columnName}";

        /// <summary>
        /// Create a new index on a table column.
        /// </summary>
        public void CreateIndex(string tableName, string columnName)
        {
            string key = MakeKey(tableName, columnName);

            if (_indexes.ContainsKey(key))
            {
                throw new InvalidOperationException($"Index on {tableName}.{columnName} already exists.");
            }

            _indexes[key] = new BPlusTree(_treeOrder);
        }

        /// <summary>
        /// Drop an existing index.
        /// </summary>
        public void DropIndex(string tableName, string columnName)
        {
            string key = MakeKey(tableName, columnName);
            _indexes.Remove(key);
        }

        /// <summary>
        /// Check whether an index exists.
        /// </summary>
        public bool HasIndex(string tableName, string columnName)
        {
            return _indexes.ContainsKey(MakeKey(tableName, columnName));
        }

        /// <summary>
        /// Insert a key -> RID into the index (if it exists).
        /// Safe to call even if index does not exist.
        /// </summary>
        public void Insert(
            string tableName,
            string columnName,
            object value,
            RID rid)
        {
            string key = MakeKey(tableName, columnName);

            if (_indexes.TryGetValue(key, out var tree))
            {
                tree.Insert(value, rid);
            }
        }

        /// <summary>
        /// Delete a key -> RID from the index (if it exists).
        /// </summary>
        public void Delete(
            string tableName,
            string columnName,
            object value,
            RID rid)
        {
            string key = MakeKey(tableName, columnName);

            if (_indexes.TryGetValue(key, out var tree))
            {
                tree.Delete(value, rid);
            }
        }

        /// <summary>
        /// Search index for an exact match.
        /// Throws if index does not exist (caller must check).
        /// </summary>
        public IEnumerable<RID> Search(
            string tableName,
            string columnName,
            object value)
        {
            string key = MakeKey(tableName, columnName);

            if (!_indexes.TryGetValue(key, out var tree))
                throw new InvalidOperationException(
                    $"No index exists on {tableName}.{columnName}");

            return tree.Search(value);
        }
    }
}
