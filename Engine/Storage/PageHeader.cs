using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DB.Engine.Storage
{
    public class PageHeader
    {
        // Fields
        int PageId { get; set; }

        PageType PageType { get; set; }

        int FreeSpaceOffset { get; set; }

        short SlotCount { get; set; }

        int CheckSum { get; set; }

        // Methods

        public void WriteTo(Span<byte> buffer)
        {
            // TODO: Implement serialization logic to write header fields to the buffer
        }

        public static PageHeader ReadFrom(ReadOnlySpan<byte> buffer)
        {
            // TODO: Implement deserialization logic to read header fields from the buffer

            return new PageHeader();
        }
    }
}
