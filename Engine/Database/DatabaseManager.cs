using DB.Engine.Storage;
using Microsoft.Win32;
using System;
using System.Linq.Expressions;
using System.Runtime.InteropServices;

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
            if (string.IsNullOrWhiteSpace(dbName))
            {
                throw new ArgumentException("Database name cannot be null or empty.");
            }
            // Validate database name
            foreach (char c in Path.GetInvalidFileNameChars())
            {
                if (dbName.Contains(c))
                {
                    throw new ArgumentException($"Database name contains invalid character: {c}");
                }
            }

            string dbPath = Path.Combine(_rootPath, dbName);

            if (Directory.Exists(dbPath))
            {
                throw new InvalidOperationException($"Database '{dbName}' already exists.");
            }

            try
            {
                // Create database directory
                Directory.CreateDirectory(dbPath);

                // Create the data file and initialize storage (this will create the meta page via FileManager)
                string dataFile = Path.Combine(dbPath, $"{dbName}_data.db");

                // FileManager creates the file and initialize a meta page in its constructor
                var fileManager = new FileManager(dataFile);

                // Ensure TableManager metadata structures are created (in-memory) if needed
                var tableManager = new TableManager(fileManager);

                // Register DB in the system catalog(persisted)
                _catalog.AddDatabase(dbName);

                // Close the FileManager to release file handles(we are not making it active yet)
                fileManager.Close();

                Console.WriteLine($"Database '{dbName}' created successfully.");
            } catch (Exception)
            {
                // Cleanup partially created files/directories
                try
                {
                    if (Directory.Exists(dbPath))
                    {
                        Directory.Delete(dbPath, true);
                    }
                } catch
                {
                    // If cleanup fails, we still rethrow the original error; but log it
                    Console.WriteLine($"Cleanup failed for partially created database folder: {dbPath}");
                }
                // Remove from catalog if added
                try
                {
                    if (_catalog.Exists(dbName))
                    {
                        _catalog.RemoveDatabase(dbName);
                    }
                }catch
                {
                    
                }
                // Rethrow original exception
                throw;

            }

        }

        /// <summary>
        /// Sets the given database as the active working context.
        /// </summary>
        public void UseDatabase(string dbName)
        {
            if (string.IsNullOrWhiteSpace(dbName))
            {
                throw new ArgumentException("Database name cannot be null or empty.");
            }

            // check if database exists
            if (!_catalog.Exists(dbName))
            {
                throw new InvalidOperationException($"Database '{dbName}' does not exist.");
            }

            string dbPath = Path.Combine(_rootPath, dbName);
            if(!Directory.Exists(dbPath))
            {
                throw new InvalidOperationException($"Database directory for '{dbName}' is missing.");
            }

            if (_activeContext != null)
            {
                Console.WriteLine($"Closing previous database '{_activeContext.DatabaseName}'...");
                // Close existing context
                _activeContext.Close();
                _activeContext = null;
            }

            _activeContext = new DatabaseContext(dbName, dbPath);

            Console.WriteLine($"Switched to database: {dbName}");
        }

        /// <summary>
        /// Deletes the given database and all its files.
        /// </summary>
        public void DropDatabase(string dbName)
        {
            if (string.IsNullOrWhiteSpace(dbName))
                throw new ArgumentException("Database name cannot be empty.", nameof(dbName));

            // Verify existence in catalog
            if (!_catalog.Exists(dbName))
                throw new InvalidOperationException($"Database '{dbName}' not found in catalog.");

            // Check physical folder
            string dbPath = Path.Combine(_rootPath, dbName);
            if (!Directory.Exists(dbPath))
                throw new InvalidOperationException($"Database folder for '{dbName}' is missing or corrupted.");

            // If currently active, close it
            if (_activeContext != null && _activeContext.DatabaseName == dbName)
            {
                Console.WriteLine($"Closing active database '{dbName}' before deletion...");
                _activeContext.Close();
                _activeContext = null;
            }

            try
            {
                // Delete the directory recursively
                Directory.Delete(dbPath, true);

                // Remove from system catalog
                _catalog.RemoveDatabase(dbName);

                Console.WriteLine($"Database '{dbName}' dropped successfully.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error while deleting database '{dbName}': {ex.Message}");
                throw;
            }
        }


        /// <summary>
        /// Lists all databases available in the root.
        /// </summary>
        public IEnumerable<string> ListDatabases(bool verbose = true)
        {
            var databases = _catalog.ListDatabases().ToList();

            if (verbose)
            {
                if (databases.Count == 0)
                {
                    Console.WriteLine("No databases found.");
                }
                else
                {
                    Console.WriteLine("\nDatabases:");
                    foreach (var db in databases)
                        Console.WriteLine($" - {db}");
                }
            }

            return databases;
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
