using DB.Engine.Database;
using DB.Engine.Storage;

namespace DB.Engine.Execution
{
    /// <summary>
    /// The RecordManager is responsible for managing record-level operations within tables.
    /// It acts as a bridge between the high-level QueryExecutor and the low-level TableManager + Page layer.
    /// </summary>
    /// <remarks>
    /// Initializes a new instance of the RecordManager.
    /// </remarks>
    /// <param name="tableManager">TableManager reference for accessing table metadata and pages.</param>
    /// <param name="fileManager">FileManager reference for reading and writing page data.</param>
    public class RecordManager(TableManager tableManager, FileManager fileManager)
    {
        private readonly TableManager _tableManager = tableManager;
        private readonly FileManager _fileManager = fileManager;

        public RecordManager(DatabaseContext dbContext) : this(dbContext.TableManager, dbContext.FileManager)
        {
            ArgumentNullException.ThrowIfNull(dbContext);
            _tableManager = dbContext.TableManager ?? throw new InvalidOperationException("DatabaseContext.TableManager is null.");
            _fileManager = dbContext.FileManager ?? throw new InvalidOperationException("DatabaseContext.FileManager is null.");
        }



        // ------------------- Methods -------------------

        /// <summary>
        /// Inserts a record into a specified table.
        /// Returns a unique RID (Record ID) if successful.
        /// </summary>
        public RID Insert(string tableName, Record record)
        {
            if (string.IsNullOrWhiteSpace(tableName))
                throw new ArgumentException("Table name cannot be empty.", nameof(tableName));

            ArgumentNullException.ThrowIfNull(record);

            // Ensure table exists
            if (!_tableManager.TableExists(tableName))
                throw new InvalidOperationException($"Table '{tableName}' does not exist.");

            // Ensure schema matches (simple check: same column count + column names)
            var schema = _tableManager.GetSchema(tableName)
                ?? throw new InvalidOperationException($"Schema for table '{tableName}' not found.");

            if (schema.ColumnCount != record.Schema.ColumnCount ||
                !schema.Columns.SequenceEqual(record.Schema.Columns))
            {
                throw new InvalidOperationException("Record schema does not match table schema.");
            }

            // Delegate actual insertion to TableManager (which handles pages, allocation, meta updates)
            var rid = _tableManager.Insert(tableName, record);

            return rid;
        }

        /// <summary>
        /// Reads a record from a specific table by its RID.
        /// </summary>
        public Record? Read(string tableName, RID rid)
        {
            if (string.IsNullOrWhiteSpace(tableName))
                throw new ArgumentException("Table name cannot be empty.", nameof(tableName));

            if (!_tableManager.TableExists(tableName))
                throw new InvalidOperationException($"Table '{tableName}' does not exist.");

            var schema = _tableManager.GetSchema(tableName)
                ?? throw new InvalidOperationException($"Schema for table '{tableName}' not found.");

            // Verify that the page belongs to this table (safety check)
            var pages = _tableManager.GetPages(tableName);
            if (!pages.Contains(rid.PageId))
                throw new InvalidOperationException($"RID page {rid.PageId} does not belong to table '{tableName}'.");

            try
            {
                // Read page bytes, load page, and read the record by slot
                var pageBytes = _fileManager.ReadPage(rid.PageId);
                var page = new Page();
                page.Load(pageBytes);

                // Use the page's ReadRecord(schema, slot) overload to get the Record
                var record = page.ReadRecord(schema, rid.SlotId);
                return record;
            }
            catch
            {
                // If reading fails (deleted slot, out of bounds, corruption), return null
                return null;
            }
        }

        /// <summary>
        /// Deletes a record from a specified table using its RID.
        /// </summary>
        public bool Delete(string tableName, RID rid)
        {
            if (string.IsNullOrWhiteSpace(tableName))
                throw new ArgumentException("Table name cannot be empty.", nameof(tableName));

            if (!_tableManager.TableExists(tableName))
                throw new InvalidOperationException($"Table '{tableName}' does not exist.");

            var pages = _tableManager.GetPages(tableName);
            if (!pages.Contains(rid.PageId))
                throw new InvalidOperationException($"RID page {rid.PageId} does not belong to table '{tableName}'.");

            // Load page
            var pageBytes = _fileManager.ReadPage(rid.PageId);
            var page = new Page();
            page.Load(pageBytes);

            // Check slot validity
            if (rid.SlotId < 0 || rid.SlotId >= page.Header.SlotCount)
                return false;

            // Read slot offset/len to know if already deleted
            int slotOffset = DbOptions.PageSize - ((rid.SlotId + 1) * 4);
            int recordOffset = System.Buffers.Binary.BinaryPrimitives.ReadInt16LittleEndian(page.Buffer.AsSpan(slotOffset + 0, 2));
            int recordLength = System.Buffers.Binary.BinaryPrimitives.ReadInt16LittleEndian(page.Buffer.AsSpan(slotOffset + 2, 2));

            if (recordOffset <= 0 || recordLength == 0)
            {
                // already deleted or empty
                return false;
            }

            // Perform delete (Page.DeleteRecord updates slot to mark deleted)
            page.DeleteRecord(rid.SlotId);

            // Persist page back to disk
            _fileManager.WritePage(rid.PageId, page.GetBytes());
            _fileManager.Flush();

            return true;
        }

        public Record BuildRecord(string tableName, IList<object?> values)
        {
            // 1) Get schema for table
            var schema = _tableManager.GetSchema(tableName) ?? throw new InvalidOperationException($"Table '{tableName}' does not exist.");

            // 2) Ensure value count matches
            if (values.Count != schema.Columns.Count)
                throw new ArgumentException($"Expected {schema.Columns.Count} values but received {values.Count}.");

            object?[] typed = new object?[values.Count];

            // 3) Validate and convert each value
            for (int i = 0; i < values.Count; i++)
            {
                object? val = values[i];
                var type = schema.ColumnTypes[i];
                bool nullable = schema.IsNullable[i];

                // NULL handling
                if (val == null)
                {
                    if (!nullable)
                        throw new ArgumentException($"Column '{schema.Columns[i]}' cannot be NULL.");
                    typed[i] = null;
                    continue;
                }

                // Type conversion
                try
                {
                    typed[i] = type switch
                    {
                        FieldType.Integer => Convert.ToInt32(val),
                        FieldType.Double => Convert.ToDouble(val),
                        FieldType.Boolean => Convert.ToBoolean(val),
                        FieldType.String => val.ToString()!,
                        _ => throw new Exception($"Unsupported field type: {type}")
                    };
                }
                catch
                {
                    throw new ArgumentException(
                        $"Value '{val}' is invalid for column '{schema.Columns[i]}' (expected {type}).");
                }
            }

            // 4) Build and return record
            return new Record(schema, typed);
        }

    }
}
