using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DB.Engine.Storage
{
    public class Page
    {
        public int  PageId { get; private set; }
        public PageHeader Header { get; private set; }
        public byte[] Buffer { get; private set; }
        public Page(int pageId, PageType type)
        {
            PageId = pageId;
            Buffer = new byte[DbOptions.PageSize]; // Initialize buffer to page size
            Header = new(pageId, type); // Create new header
            FlushHeader(); // Write header to buffer
        }


        // Methods

        void Load(byte[] data)
        {
            if (data.Length != DbOptions.PageSize)
            {
                throw new ArgumentException("Corrupted Data");
            }

            Array.Copy(data, Buffer, data.Length); // Copy data into buffer
            Header = PageHeader.ReadFrom(Buffer); // Read header from buffer

            if (Header.PageType != PageType.Data &&
                Header.PageType != PageType.Index &&
                Header.PageType != PageType.Meta &&
                Header.PageType != PageType.Free) // Validate page type
            {
                throw new ArgumentException("Invalid Page Type");
            }

            if (Header.SlotCount < 0 || Header.SlotCount > 1024) // Validate slot count
            {
                throw new ArgumentException("Corrupted Slot Count");
            }

            PageId = Header.PageId;
        }

        byte[] GetBytes()
        {
            // Return the byte array representation of the page
            return Buffer;
        }

        int GetFreeSpace()
        {
            if (Header.FreeSpaceOffset > DbOptions.PageSize || Header.SlotCount < 0)
                return 0;  // corrupted page or bad header

            short SlotBytesUsed = (short)(Header.SlotCount * 4);

            int FreeSpace = DbOptions.PageSize - SlotBytesUsed - Header.FreeSpaceOffset;  // page size - slot directory size - used space

            return FreeSpace;
        }

        /*
         * TODO:
         * 
         * add functionality to handle record fragmentation and compaction
         * reuse slots of deleted records
         * 
         */
        bool TryInsertRecord(byte[] payload, out int slotIndex)
        {

            if (payload == null || payload.Length == 0)
            {
                slotIndex = -1;
                return false;
            }

            int RecordLength = payload.Length;

            if (RecordLength + 4 > GetFreeSpace())
            {
                slotIndex = -1;
                return false; // Not enough space
            }
            // Insert the record and update header


            if (Header.SlotCount >= 1024)
            {
                slotIndex = -1;
                return false; // Too many records in this page
            }

            int RecordOffset = Header.FreeSpaceOffset;
            Array.Copy(payload, 0, Buffer, RecordOffset, RecordLength); // Copy payload to buffer
            Header.FreeSpaceOffset += RecordLength; // Update free space offset

            // Update slot directory
            int SlotOffset = DbOptions.PageSize - ((Header.SlotCount + 1) * 4);
            BinaryPrimitives.WriteInt16LittleEndian(Buffer.AsSpan(SlotOffset + 0, 2), (short)RecordOffset); // Write record offset to slot
            BinaryPrimitives.WriteInt16LittleEndian(Buffer.AsSpan(SlotOffset + 2, 2), (short)RecordLength); // Write record length to slot
            Header.SlotCount += 1;
            slotIndex = Header.SlotCount - 1;
            
            FlushHeader(); // Update header in buffer
            return true;
        }

        byte[] ReadRecord(int slotId)
        {
            if (slotId < 0 || slotId >= Header.SlotCount)
            {
                throw new ArgumentOutOfRangeException(nameof(slotId), "Invalid slot ID");
            }

            int SlotOffset = DbOptions.PageSize - ((slotId + 1) * 4);
            int RecordOffset = BinaryPrimitives.ReadInt16LittleEndian(Buffer.AsSpan(SlotOffset + 0, 2));
            int RecordLength = BinaryPrimitives.ReadInt16LittleEndian(Buffer.AsSpan(SlotOffset + 2, 2));

            if (RecordOffset == 0)
            {
                throw new InvalidOperationException("Slot is empty or deleted");
            }

            if (RecordOffset < DbOptions.HeaderSize || RecordOffset + RecordLength > DbOptions.PageSize)
            { 
                throw new InvalidOperationException("Corrupted slot entry or record out of bounds");
            }

            byte[] record = new byte[RecordLength];
            Array.Copy(Buffer, RecordOffset, record, 0, RecordLength);

            return record;
        }

        void DeleteRecord(int slotId)
        {
            if (slotId < 0 || slotId >= Header.SlotCount)
            {
                throw new ArgumentOutOfRangeException(nameof(slotId));
            }

            int SlotOffset = DbOptions.PageSize - ((slotId + 1) * 4);
            int RecordOffset = BinaryPrimitives.ReadInt16LittleEndian(Buffer.AsSpan(SlotOffset + 0, 2));
            int RecordLength = BinaryPrimitives.ReadInt16LittleEndian(Buffer.AsSpan(SlotOffset + 2, 2));
            
            if (RecordOffset == 0 && RecordLength == 0)
            {
                return; // Already deleted
            }

            BinaryPrimitives.WriteInt16LittleEndian(Buffer.AsSpan(SlotOffset + 0, 2), 0); // Mark record offset as 0
            BinaryPrimitives.WriteInt16LittleEndian(Buffer.AsSpan(SlotOffset + 2, 2), 0); // Mark record length as 0

            FlushHeader(); // Update header in buffer
        }

        void FlushHeader()
        {
            Header.WriteTo(Buffer.AsSpan(0, DbOptions.HeaderSize));
        }
    }
}
