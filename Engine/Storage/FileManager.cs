using System;
using System.IO;

namespace DB.Engine.Storage
{
    public class FileManager : IDisposable
    {
        private readonly FileStream _stream;
        private readonly string _dbFilePath;
        private int _pageCount;

        public int PageCount => _pageCount; // expose for reading
        public string DatabasePath => _dbFilePath; // optional helper

        // Constructor
        public FileManager(string path)
        {
            _dbFilePath = path;

            // If DB file doesn't exist → create new one
            if (!File.Exists(_dbFilePath))
            {
                _stream = new FileStream(_dbFilePath, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None);
                InitializeNewDatabase();
            }
            else
            {
                // Open existing DB file
                _stream = new FileStream(_dbFilePath, FileMode.Open, FileAccess.ReadWrite, FileShare.None);

                // Validate file integrity (must align with page size)
                if (_stream.Length % DbOptions.PageSize != 0)
                    throw new InvalidDataException("Corrupted database file: size not aligned with page size.");

                _pageCount = (int)(_stream.Length / DbOptions.PageSize);
            }
        }

        // Static factory for convenience
        public static FileManager OpenOrCreate(string path)
        {
            return new FileManager(path);
        }

        // Read a page from the file by its ID
        public byte[] ReadPage(int pageId)
        {
            if (pageId < 0 || pageId >= _pageCount)
                throw new ArgumentOutOfRangeException(nameof(pageId), "Invalid page ID.");

            var buffer = new byte[DbOptions.PageSize];
            long offset = (long)pageId * DbOptions.PageSize;

            _stream.Seek(offset, SeekOrigin.Begin);
            int bytesRead = _stream.Read(buffer, 0, DbOptions.PageSize);

            if (bytesRead != DbOptions.PageSize)
                throw new IOException("Failed to read full page from file (possible corruption).");

            return buffer;
        }

        // Write a full page to disk at a given ID
        public void WritePage(int pageId, byte[] data)
        {
            ArgumentNullException.ThrowIfNull(data);

            if (data.Length != DbOptions.PageSize)
                throw new ArgumentException($"Invalid page size. Expected {DbOptions.PageSize} bytes.");

            long offset = (long)pageId * DbOptions.PageSize;

            _stream.Seek(offset, SeekOrigin.Begin);
            _stream.Write(data, 0, DbOptions.PageSize);
        }

        // Allocate a new blank page of given type and return its ID
        public int AllocatePage(PageType type)
        {
            int newPageId = _pageCount;
            var page = new Page(newPageId, type);

            WritePage(newPageId, page.Buffer);
            _pageCount++;

            return newPageId;
        }

        // Flush changes to disk
        public void Flush()
        {
            _stream.Flush(true);
        }

        // Close or dispose file
        public void Close()
        {
            _stream.Flush(true);
            _stream.Dispose();
        }

        public void Dispose()
        {
            Close();
        }

        // Initialize a brand new database with a Meta page
        private void InitializeNewDatabase()
        {
            var metaPage = new Page(0, PageType.Meta);

            _stream.Seek(0, SeekOrigin.Begin);
            _stream.Write(metaPage.Buffer, 0, DbOptions.PageSize);
            _stream.Flush(true);

            _pageCount = 1;
        }
    }
}
