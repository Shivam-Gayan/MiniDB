using DB.Engine.Storage;

namespace DB.Engine.Database
{
    /// <summary>
    /// Represents the active database context.
    /// Holds instances of FileManager, TableManager, and database-specific configuration.
    /// </summary>
    public class DatabaseContext
    {
        public string DatabaseName { get; }
        public string DatabasePath { get; }

        public FileManager FileManager { get; }
        public TableManager TableManager { get; }

        public DatabaseContext(string dbName, string dbPath)
        {
            DatabaseName = dbName;
            DatabasePath = dbPath;

            string dataFile = Path.Combine(dbPath, $"{dbName}_data.db");
            FileManager = new FileManager(dataFile);
            TableManager = new TableManager(FileManager);
        }

        /// <summary>
        /// Closes the current database context and releases resources.
        /// </summary>
        public void Close()
        {
            FileManager.Close();
        }
    }
}
