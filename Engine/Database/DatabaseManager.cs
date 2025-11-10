using DB.Engine.Storage;

namespace DB.Engine.Database
{
    /// <summary>
    /// The DatabaseManager class manages multiple databases within the DBMS root directory.
    /// It handles creation, deletion, listing, and switching between databases.
    /// </summary>
    public class DatabaseManager
    {
        private readonly string _rootPath;
        private DatabaseContext? _activeContext;
        private readonly SystemCatalog _catalog;

        /// <summary>
        /// Initializes the DatabaseManager with the root directory.
        /// </summary>
        public DatabaseManager(string rootPath = "DBRoot")
        {
            _rootPath = rootPath;
            Directory.CreateDirectory(_rootPath);
            _catalog = new SystemCatalog(_rootPath);
        }

        // ------------------- Database Operations -------------------

        /// <summary>
        /// Creates a new database and initializes its folder structure.
        /// </summary>
        public void CreateDatabase(string dbName)
        {
            // TODO: Implement creation logic using SystemCatalog
        }

        /// <summary>
        /// Sets the given database as the active working context.
        /// </summary>
        public void UseDatabase(string dbName)
        {
            // TODO: Implement logic to open and set context
        }

        /// <summary>
        /// Deletes the given database and all its files.
        /// </summary>
        public void DropDatabase(string dbName)
        {
            // TODO: Implement delete logic using SystemCatalog
        }

        /// <summary>
        /// Lists all databases available in the root.
        /// </summary>
        public IEnumerable<string> ListDatabases()
        {
            // TODO: Return list of database names
            return Enumerable.Empty<string>();
        }

        /// <summary>
        /// Returns the active database context.
        /// </summary>
        public DatabaseContext GetActiveContext()
        {
            if (_activeContext == null)
                throw new Exception("No active database. Use 'UseDatabase()' first.");
            return _activeContext;
        }

        /// <summary>
        /// Returns the name of the currently active database.
        /// </summary>
        public string? ActiveDatabase => _activeContext?.DatabaseName;
    }
}
