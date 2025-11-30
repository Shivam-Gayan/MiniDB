using DB.Engine.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DB.Engine.Index
{
    /// <summary>
    /// Base abstract node for the B+Tree.
    /// Keeps Keys in sorted order (List&lt;Key&gt;).
    /// PageId defaults to -1 (meaning not allocated on disk yet).
    /// </summary>
    internal abstract class Node
    {
        /// <summary>
        /// PageId reserved for future on-disk mapping. -1 means in-memory only.
        /// </summary>
        public int PageId { get; set; } = -1;

        /// <summary>
        /// Parent node pointer (null for root).
        /// </summary>
        public Node? Parent { get; set; }

        /// <summary>
        /// Whether this node is a leaf node.
        /// </summary>
        public abstract bool IsLeaf { get; }

        /// <summary>
        /// Sorted keys inside the node. For an internal node with N keys there are N+1 children.
        /// For a leaf node, Values has the same length as Keys.
        /// </summary>
        public readonly List<Key> Keys = [];
    }

    /// <summary>
    /// Internal node: contains keys and child pointers.
    /// children.Count == keys.Count + 1 always (after proper construction).
    /// </summary>
    internal sealed class InternalNode : Node
    {
        public override bool IsLeaf => false;

        /// <summary>
        /// Child pointers. Stored in-memory as Node references; when persisted these will map to PageIds.
        /// </summary>
        public readonly List<Node> Children = [];
    }

    /// <summary>
    /// Leaf node: stores actual record references (RID lists) alongside keys.
    /// Values[i] is the list of RIDs for Keys[i].
    /// Leaves are doubly-linked for fast range scans.
    /// </summary>
    internal sealed class LeafNode : Node
    {
        public override bool IsLeaf => true;

        /// <summary>
        /// Parallel to Keys: Values[i] is the list of RIDs for Keys[i].
        /// </summary>
        public readonly List<List<RID>> Values = [];

        /// <summary>
        /// Linked-list pointer to the next leaf (null if last).
        /// </summary>
        public LeafNode? Next { get; set; }

        /// <summary>
        /// Linked-list pointer to the previous leaf (null if first).
        /// </summary>
        public LeafNode? Prev { get; set; }
    }
}
