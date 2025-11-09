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

        /// <summary>
        /// Scans all pages of the given table and yields valid records.
        /// </summary>
        /// <param name="tableName">Name of the table to scan.</param>
        /// <returns>An enumerable sequence of Record objects.</returns>
        public IEnumerable<Record> ScanTable(string tableName)
        {
            // TODO: Implement full table scan logic
            yield break;
        }
    }
}
