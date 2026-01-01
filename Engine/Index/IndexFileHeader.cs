using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Reflection.PortableExecutable;
using System.Text;
using System.Threading.Tasks;

namespace DB.Engine.Index
{
    internal sealed class IndexFileHeader
    {
        public const int MagicValue = 0x31445849; // "IDX1"

        public int PageSize { get; set; }
        public int IndexCount { get; set; }
        public int FirstMetaPageId { get; set; }

        public void WriteTo(Span<byte> buffer)
        {
            BinaryPrimitives.WriteInt32LittleEndian(buffer.Slice(0, 4), MagicValue);
            BinaryPrimitives.WriteInt32LittleEndian(buffer.Slice(4, 4), PageSize);
            BinaryPrimitives.WriteInt32LittleEndian(buffer.Slice(8, 4), IndexCount);
            BinaryPrimitives.WriteInt32LittleEndian(buffer.Slice(12, 4), FirstMetaPageId);
        }

        public static IndexFileHeader ReadFrom(ReadOnlySpan<byte> buffer)
        {
            int magic = BinaryPrimitives.ReadInt32LittleEndian(buffer.Slice(0, 4));
            if (magic != MagicValue)
                throw new InvalidDataException("Invalid index file.");

            return new IndexFileHeader
            {
                PageSize = BinaryPrimitives.ReadInt32LittleEndian(buffer.Slice(4, 4)),
                IndexCount = BinaryPrimitives.ReadInt32LittleEndian(buffer.Slice(8, 4)),
                FirstMetaPageId = BinaryPrimitives.ReadInt32LittleEndian(buffer.Slice(12, 4))
            };
        }
    }
}
