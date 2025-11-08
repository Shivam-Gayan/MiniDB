using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DB.Engine.Storage
{
    public readonly struct RID
    {
        public int PageId { get; }
        public int SlotId { get; }

        public RID(int pageId, int slotId)
        {
            PageId = pageId;
            SlotId = slotId;
        }

        public override string ToString() => $"RID(PageId: {PageId}, SlotId: {SlotId})";

    }
}
