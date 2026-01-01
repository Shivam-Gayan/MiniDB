using System.Buffers.Binary;
using DB.Engine.Storage;

namespace DB.Engine.Indexing
{
    internal static class IndexNodeHeader
    {
        public const int Offset = DbOptions.HeaderSize;
        public const int Size = 1 + 2 + 4; // IsLeaf + KeyCount + ParentPageId

        public static void Write(
            Span<byte> buffer,
            bool isLeaf,
            short keyCount,
            int parentPageId)
        {
            buffer[Offset] = (byte)(isLeaf ? 1 : 0);
            BinaryPrimitives.WriteInt16LittleEndian(buffer.Slice(Offset + 1, 2), keyCount);
            BinaryPrimitives.WriteInt32LittleEndian(buffer.Slice(Offset + 3, 4), parentPageId);
        }

        public static void Read(
            ReadOnlySpan<byte> buffer,
            out bool isLeaf,
            out short keyCount,
            out int parentPageId)
        {
            isLeaf = buffer[Offset] == 1;
            keyCount = BinaryPrimitives.ReadInt16LittleEndian(buffer.Slice(Offset + 1, 2));
            parentPageId = BinaryPrimitives.ReadInt32LittleEndian(buffer.Slice(Offset + 3, 4));
        }
    }
}
