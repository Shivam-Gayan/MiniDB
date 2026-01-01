using System.Buffers.Binary;


namespace DB.Engine.Storage
{
    public class PageHeader
    {
        // Fields
        public int PageId { get; set; }

        public PageType PageType { get; set; }

        public int FreeSpaceOffset { get; set; }

        public short SlotCount { get; set; }

        public int CheckSum { get; set; }

        public PageHeader(int pageId, PageType type)
        {
            PageId = pageId;
            PageType = type;
            FreeSpaceOffset = DbOptions.HeaderSize;
            SlotCount = 0;
            CheckSum = 0;
        }

        // Methods

        public void WriteTo(Span<byte> buffer)
        {
            if (buffer.Length < DbOptions.HeaderSize)
                throw new ArgumentException($"Buffer must be atleast {DbOptions.HeaderSize} bytes");

            buffer.Slice(0, DbOptions.HeaderSize).Clear();

            BinaryPrimitives.WriteInt32LittleEndian(buffer.Slice(0, 4), PageId);
            buffer[4] = (byte)PageType;

            if (PageType == PageType.Data || PageType == PageType.Meta)
            {
                BinaryPrimitives.WriteInt32LittleEndian(buffer.Slice(8, 4), FreeSpaceOffset);
                BinaryPrimitives.WriteInt16LittleEndian(buffer.Slice(12, 2), SlotCount);
            }

            BinaryPrimitives.WriteInt32LittleEndian(buffer.Slice(16, 4), CheckSum);
        }

        public static PageHeader ReadFrom(ReadOnlySpan<byte> buffer)
        {
            if (buffer.Length < DbOptions.HeaderSize)
                throw new ArgumentException($"Buffer must be atleast {DbOptions.HeaderSize} bytes");

            int pageId = BinaryPrimitives.ReadInt32LittleEndian(buffer.Slice(0, 4));
            PageType pageType = (PageType)buffer[4];

            var header = new PageHeader(pageId, pageType);

            // ONLY slotted pages read slot metadata
            if (pageType == PageType.Data || pageType == PageType.Meta)
            {
                header.FreeSpaceOffset =
                    BinaryPrimitives.ReadInt32LittleEndian(buffer.Slice(8, 4));
                header.SlotCount =
                    BinaryPrimitives.ReadInt16LittleEndian(buffer.Slice(12, 2));
            }
            else
            {
                // Index / Free pages → no slot semantics
                header.FreeSpaceOffset = DbOptions.HeaderSize;
                header.SlotCount = 0;
            }

            header.CheckSum =
                BinaryPrimitives.ReadInt32LittleEndian(buffer.Slice(16, 4));

            return header;
        }

    }
}
