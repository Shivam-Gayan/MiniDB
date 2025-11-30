using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DB.Engine.Storage
{
    public readonly struct RID(int pageId, int slotId)
    {
        public int PageId { get; } = pageId;
        public int SlotId { get; } = slotId;

        public override string ToString() => $"RID(PageId: {PageId}, SlotId: {SlotId})";

    }
}
