using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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

        BPlusTree(int order)
        {
            if (order < 3)
                throw new ArgumentException("B+Tree order must be at least 3.", nameof(order));
            Order = order;
            _root = new LeafNode();
        }


    }
}
