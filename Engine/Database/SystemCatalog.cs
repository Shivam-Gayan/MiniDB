using System.Text.Json;

namespace DB.Engine.Database
{
    /// <summary>
    /// Manages the list of all databases in the DBMS root directory.
    /// Stores and loads metadata (database names, creation time, etc.).
    /// </summary>
    public class SystemCatalog
    {
        private readonly string _catalogFilePath;
        private Dictionary<string, DateTime> _databases;

        public SystemCatalog(string rootPath)
        {
            _catalogFilePath = Path.Combine(rootPath, "db_catalog.json");
            Load();
        }

        /// <summary>
        /// Checks if a given database exists in the catalog.
        /// </summary>
        public bool Exists(string dbName)
        {
            return _databases.ContainsKey(dbName);
        }


        /// <summary>
        /// Adds a new database to the catalog.
        /// </summary>
        public void AddDatabase(string dbName)
        {
            if (_databases.ContainsKey(dbName))
            {
                throw new InvalidOperationException($"Database '{dbName}' already exists.");
            }

            _databases[dbName] = DateTime.UtcNow;
            Save();
        }

        /// <summary>
        /// Removes a database from the catalog.
        /// </summary>
        public void RemoveDatabase(string dbName)
        {
            if (_databases.Remove(dbName))
                Save();
        }

        /// <summary>
        /// Returns all database names.
        /// </summary>
        public IEnumerable<string> ListDatabases() => _databases.Keys;

        /// <summary>
        /// Loads catalog data from disk.
        /// </summary>
        private void Load()
        {
            if (!File.Exists(_catalogFilePath))
            {
                _databases = [];
                Save();
                return;
            }

            try {
                string json = File.ReadAllText(_catalogFilePath);
                _databases = JsonSerializer.Deserialize<Dictionary<string, DateTime>>(json)
                    ?? [];
            }
            catch(Exception)
            {
                Console.WriteLine("Warning: Failed to load system catalog. Initializing empty catalog.");
                _databases = [];
                Save();
            }
        }

        /// <summary>
        /// Saves catalog data to disk.
        /// </summary>
        private void Save()
        {
            string json = JsonSerializer.Serialize(_databases, new JsonSerializerOptions { WriteIndented = true });
            File.WriteAllText(_catalogFilePath, json);
        }
    }
}
