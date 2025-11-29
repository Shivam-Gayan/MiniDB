using DB.Engine.Database;
using DB.Engine.Storage;

namespace DB.Engine.Execution
{
    /// <summary>
    /// The TableScan class performs a sequential scan over all pages in a given table.
    /// It reads records in order, skipping deleted ones.
    /// </summary>
    /// <remarks>
    /// Initializes a new TableScan object for iterating through records.
    /// </remarks>
    public class TableScan(TableManager tableManager, FileManager fileManager)
    {
        private readonly TableManager _tableManager = tableManager;
        private readonly FileManager _fileManager = fileManager;

        public TableScan(DatabaseContext dbContext) : this(dbContext.TableManager, dbContext.FileManager)
        {
            _tableManager = dbContext.TableManager;
            _fileManager = dbContext.FileManager;
        }

        /// <summary>
        /// Scans all pages of the given table and yields valid records.
        /// </summary>
        /// <param name="tableName">Name of the table to scan.</param>
        /// <returns>An enumerable sequence of Record objects.</returns>
        public IEnumerable<Record> ScanTable(string tableName)
        {
            if (!_tableManager.TableExists(tableName))
            {
                throw new ArgumentException($"Table '{tableName}' does not exist.");
            }

            // Get Schema and total pages
            var schema = _tableManager.GetSchema(tableName) ?? throw new InvalidOperationException("Schema not found for this table.");
            List<int> totalPages = _tableManager.GetPages(tableName);

            // Iterate through each page
            foreach (var pageId in totalPages)
            {
                // read page from disk
                byte[] pageData = _fileManager.ReadPage(pageId);

                // load page
                var page = new Page();
                page.Load(pageData);

                // iterate through each slot in the page
                for (int slotId = 0; slotId < page.Header.SlotCount; slotId++)
                {

                    Record? record = null;

                    try
                    {
                        // read record and deserialize
                        record = page.ReadRecord(schema, slotId);
                    }
                    catch
                    {
                        continue; // skip deleted or invalid records
                    }

                    yield return record;
                }
            }
        }
    }
}
