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
            {
                throw new ArgumentException($"Buffer must be atleast {DbOptions.HeaderSize} bytes");
            }

            buffer.Slice(0, DbOptions.HeaderSize).Clear(); // Clear header area

            BinaryPrimitives.WriteInt32LittleEndian(buffer.Slice(0, 4), PageId); // 4 byte for PageId

            buffer[4] = (byte)PageType; // 1 byte for PageType

            // 3 bytes padding 5 6 7

            BinaryPrimitives.WriteInt32LittleEndian(buffer.Slice(8, 4), FreeSpaceOffset); // 4 bytes for FreeSpaceOffset

            BinaryPrimitives.WriteInt16LittleEndian(buffer.Slice(12, 2), SlotCount); // 2 bytes for SlotCount

            // 2 bytes padding 14 15

            BinaryPrimitives.WriteInt32LittleEndian(buffer.Slice(16, 4), CheckSum); // 4 bytes for CheckSum

            // 20 bytes padding 20-31
        }

        public static PageHeader ReadFrom(ReadOnlySpan<byte> buffer)
        {
            if (buffer.Length < DbOptions.HeaderSize)
            {
                throw new ArgumentException($"Buffer must be atleast {DbOptions.HeaderSize} bytes");
            }

            // Read fields from buffer
            int pageId = BinaryPrimitives.ReadInt32LittleEndian(buffer.Slice(0, 4));
            PageType pageType = (PageType)buffer[4];
            int freeSpaceOffset = BinaryPrimitives.ReadInt32LittleEndian(buffer.Slice(8, 4));
            short slotCount = BinaryPrimitives.ReadInt16LittleEndian(buffer.Slice(12, 2));
            int checkSum = BinaryPrimitives.ReadInt32LittleEndian(buffer.Slice(16, 4));

            var header = new PageHeader(pageId, pageType)
            {
                FreeSpaceOffset = freeSpaceOffset,
                SlotCount = slotCount,
                CheckSum = checkSum
            };

            return header;
        }
    }
}
