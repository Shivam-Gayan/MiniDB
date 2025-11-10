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

        public Page() 
        {
            Buffer = new byte[DbOptions.PageSize];
            Header = new PageHeader(0, PageType.Free); // default placeholder
            FlushHeader();
        }



        // Methods

        public void Load(byte[] data)
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

        public byte[] GetBytes()
        {
            // Return the byte array representation of the page
            return Buffer;
        }

        public int GetFreeSpace()
        {
            if (Header.FreeSpaceOffset > DbOptions.PageSize || Header.SlotCount < 0)
                return 0;  // corrupted page or bad header

            short SlotBytesUsed = (short)(Header.SlotCount * 4);

            int FreeSpace = DbOptions.PageSize - SlotBytesUsed - Header.FreeSpaceOffset;  // page size - slot directory size - used space

            return FreeSpace;
        }

        public bool TryInsertRecord(byte[] payload, out int slotIndex)
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

            if (Header.SlotCount >= 1024)
            {
                slotIndex = -1;
                return false; // Too many records in this page
            }

            // Check for reusable slots from deleted records
            int reusableSlotIndex = -1;

            for (int i = 0; i < Header.SlotCount; i++)
            {
                int slotOffset = DbOptions.PageSize - ((i + 1) * 4);
                int recordOffset = BinaryPrimitives.ReadInt16LittleEndian(Buffer.AsSpan(slotOffset + 0, 2));
                int recordLength = BinaryPrimitives.ReadInt16LittleEndian(Buffer.AsSpan(slotOffset + 2, 2));

                if (recordOffset <= 0 && recordLength == 0)
                {
                    reusableSlotIndex = i;
                    break;
                }

            }

            // write record to buffer

            int RecordOffset = Header.FreeSpaceOffset;
            Array.Copy(payload, 0, Buffer, RecordOffset, RecordLength); // Copy payload to buffer
            Header.FreeSpaceOffset += RecordLength; // Update free space offset

            if (reusableSlotIndex != -1)
            {
                // Reuse deleted slot

                int SlotOffset = DbOptions.PageSize - ((reusableSlotIndex + 1) * 4);
                BinaryPrimitives.WriteInt16LittleEndian(Buffer.AsSpan(SlotOffset + 0, 2), (short)RecordOffset); // Write record offset to slot
                BinaryPrimitives.WriteInt16LittleEndian(Buffer.AsSpan(SlotOffset + 2, 2), (short)RecordLength); // Write record length to slot
                slotIndex = reusableSlotIndex;

            } else
            {

                // Update slot directory

                int SlotOffset = DbOptions.PageSize - ((Header.SlotCount + 1) * 4);
                BinaryPrimitives.WriteInt16LittleEndian(Buffer.AsSpan(SlotOffset + 0, 2), (short)RecordOffset); // Write record offset to slot
                BinaryPrimitives.WriteInt16LittleEndian(Buffer.AsSpan(SlotOffset + 2, 2), (short)RecordLength); // Write record length to slot
                Header.SlotCount += 1;
                slotIndex = Header.SlotCount - 1;

            }
            
            FlushHeader(); // Update header in buffer
            return true;
        }

        public bool TryInsertRecord(Record record, out int slotIndex) // overload for Record type to byte[]
        {
            byte[] payload = record.ToBytes();
            return TryInsertRecord(payload, out slotIndex);
        }

        public byte[] ReadRecord(int slotId)
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

        public Record ReadRecord(Schema schema, int slotId) // overload for Record type to byte[]
        {
            byte[] recordBytes = ReadRecord(slotId);
            return Record.FromBytes(schema, recordBytes);
        } 

        public void DeleteRecord(int slotId)
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

            BinaryPrimitives.WriteInt16LittleEndian(Buffer.AsSpan(SlotOffset + 0, 2), -1); // Mark record offset as -1 (deleted) 
            BinaryPrimitives.WriteInt16LittleEndian(Buffer.AsSpan(SlotOffset + 2, 2), 0); // Mark record length as 0

            FlushHeader(); // Update header in buffer
        }

        public void Vacuum()
        {
            // Step 1: Collect all valid records
            var liveRecords = new List<(int SlotId, byte[] Data)>();

            for (int slot = 0; slot < Header.SlotCount; slot++)
            {
                int slotOffset = DbOptions.PageSize - ((slot + 1) * 4);
                int recordOffset = BinaryPrimitives.ReadInt16LittleEndian(Buffer.AsSpan(slotOffset, 2));
                int recordLength = BinaryPrimitives.ReadInt16LittleEndian(Buffer.AsSpan(slotOffset + 2, 2));

                if (recordOffset <= 0 || recordLength == 0)
                    continue; // Skip deleted/free slots

                byte[] recordData = new byte[recordLength];
                Array.Copy(Buffer, recordOffset, recordData, 0, recordLength);

                liveRecords.Add((slot, recordData));
            }

            // Step 2: Compact data region
            int newOffset = DbOptions.HeaderSize;

            foreach (var (slotId, recordData) in liveRecords)
            {
                Array.Copy(recordData, 0, Buffer, newOffset, recordData.Length);

                int slotOffset = DbOptions.PageSize - ((slotId + 1) * 4);
                BinaryPrimitives.WriteInt16LittleEndian(Buffer.AsSpan(slotOffset, 2), (short)newOffset);
                BinaryPrimitives.WriteInt16LittleEndian(Buffer.AsSpan(slotOffset + 2, 2), (short)recordData.Length);

                newOffset += recordData.Length;
            }

            // Step 3: Reset free space offset
            Header.FreeSpaceOffset = newOffset;

            // Step 4: (Optional) Zero out reclaimed space
            int remaining = DbOptions.PageSize - newOffset - (Header.SlotCount * 4);
            if (remaining > 0)
                Array.Clear(Buffer, newOffset, remaining);

            // Step 5: Flush header
            FlushHeader();

            Console.WriteLine($"Page {Header.PageId} vacuumed successfully. Freed space up to offset {newOffset}.");
        }


        public void FlushHeader()
        {
            Header.WriteTo(Buffer.AsSpan(0, DbOptions.HeaderSize));
        }
    }
}
