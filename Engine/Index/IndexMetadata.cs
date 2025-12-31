using DB.Engine.Storage;
using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DB.Engine.Index
{
    internal sealed class IndexMetadata
    {
        public int IndexId;
        public FieldType KeyType;
        public string ColumnName = "";
        public int RootPageId;
        public int Order;
        public int NextMetaPageId;

        public void WriteTo(Span<byte> buffer)
        {
            int offset = DbOptions.HeaderSize;

            BinaryPrimitives.WriteInt32LittleEndian(buffer.Slice(offset, 4), IndexId);
            offset += 4;

            buffer[offset++] = (byte)KeyType;

            var nameBytes = Encoding.UTF8.GetBytes(ColumnName);
            buffer[offset++] = (byte)nameBytes.Length;
            nameBytes.CopyTo(buffer.Slice(offset));
            offset += nameBytes.Length;

            BinaryPrimitives.WriteInt32LittleEndian(buffer.Slice(offset, 4), RootPageId);
            offset += 4;

            BinaryPrimitives.WriteInt32LittleEndian(buffer.Slice(offset, 4), Order);
            offset += 4;

            BinaryPrimitives.WriteInt32LittleEndian(buffer.Slice(offset, 4), NextMetaPageId);
        }

        public static IndexMetadata ReadFrom(ReadOnlySpan<byte> buffer)
        {
            int offset = DbOptions.HeaderSize;

            var meta = new IndexMetadata
            {
                IndexId = BinaryPrimitives.ReadInt32LittleEndian(buffer.Slice(offset, 4))
            };
            offset += 4;

            meta.KeyType = (FieldType)buffer[offset++];

            int len = buffer[offset++];
            meta.ColumnName = Encoding.UTF8.GetString(buffer.Slice(offset, len));
            offset += len;

            meta.RootPageId = BinaryPrimitives.ReadInt32LittleEndian(buffer.Slice(offset, 4));
            offset += 4;

            meta.Order = BinaryPrimitives.ReadInt32LittleEndian(buffer.Slice(offset, 4));
            offset += 4;

            meta.NextMetaPageId = BinaryPrimitives.ReadInt32LittleEndian(buffer.Slice(offset, 4));

            return meta;
        }
    }

}
