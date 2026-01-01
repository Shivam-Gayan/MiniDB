using DB.Engine.Index;
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
        public IndexManager IndexManager { get; }
        private readonly Dictionary<string, IndexFileHandle> _indexHandles = new();

        public DatabaseContext(string dbName, string dbPath)
        {
            DatabaseName = dbName ?? throw new ArgumentNullException(nameof(dbName));
            DatabasePath = dbPath ?? throw new ArgumentNullException(nameof(dbPath));

            string dataFile = Path.Combine(dbPath, $"{dbName}_data.db");
            FileManager = new FileManager(dataFile);
            TableManager = new TableManager(FileManager);
            IndexManager = new IndexManager(this);
        }

        /// <summary>
        /// Closes the current database context and releases resources.
        /// </summary>
        public void Close()
        {
            try
            {
                foreach (var handle in _indexHandles.Values)
                    handle.FileManager.Dispose();

                _indexHandles.Clear();

                FileManager.Dispose();
            }
            catch
            {
                // shutdown safety
            }
        }

        public IndexFileHandle GetIndexFileHandle(string tableName)
        {
            if (_indexHandles.TryGetValue(tableName, out var handle))
                return handle;

            string indexPath = Path.Combine(
                DatabasePath,
                $"{DatabaseName}_{tableName}.idx");

            var fm = new FileManager(indexPath,true);

            // PAGE 0 = IndexFileHeader
            IndexFileHeader header;

            if (fm.PageCount == 1) // freshly created file
            {
                header = new IndexFileHeader
                {
                    PageSize = DbOptions.PageSize,
                    IndexCount = 0,
                    FirstMetaPageId = -1
                };

                var headerPage = new Page(0, PageType.Index);
                header.WriteTo(headerPage.Buffer);
                fm.WritePage(0, headerPage.Buffer);
            }
            else
            {
                var headerPage = new Page();
                headerPage.Load(fm.ReadPage(0));
                header = IndexFileHeader.ReadFrom(headerPage.Buffer);

                // safety check
                if (header.PageSize != DbOptions.PageSize)
                    throw new InvalidDataException("Index page size mismatch.");
            }

            handle = new IndexFileHandle(fm, header.FirstMetaPageId);
            _indexHandles[tableName] = handle;

            return handle;
        }

    }
}
