using DB.Engine.Storage;

namespace DB.Engine.Execution
{
    /// <summary>
    /// The QueryExecutor is the high-level entry point for executing database operations.
    /// It orchestrates commands like INSERT, SELECT, and DELETE by delegating to RecordManager and TableScan.
    /// </summary>
    public class QueryExecutor
    {
        private readonly RecordManager _recordManager;
        private readonly TableScan _tableScan;
        private readonly TableManager _tableManager;
        private readonly FileManager _fileManager;

        /// <summary>
        /// Initializes a new QueryExecutor instance.
        /// </summary>
        public QueryExecutor(TableManager tableManager, FileManager fileManager)
        {
            _tableManager = tableManager;
            _fileManager = fileManager;
            _recordManager = new RecordManager(_tableManager, _fileManager);
            _tableScan = new TableScan(_tableManager, _fileManager);
        }

        // ------------------- High-level Commands -------------------

        /// <summary>
        /// Executes an INSERT operation into a given table.
        /// </summary>
        public RID Insert(string tableName, Record record)
        {
            return _recordManager.Insert(tableName, record);
        }

        /// <summary>
        /// Executes a SELECT * FROM Table operation.
        /// </summary>
        public IEnumerable<Record> SelectAll(string tableName)
        {
            return _tableScan.ScanTable(tableName);
        }

        /// <summary>
        /// Executes a DELETE operation for a given record.
        /// </summary>
        public bool Delete(string tableName, RID rid)
        {
            return _recordManager.Delete(tableName, rid);
        }
    }
}
