using System;
using System.IO;
using System.Text.Json.Serialization;

namespace DB.Engine.Database
{
    /// <summary>
    /// Represents metadata for a single database inside the DBMS.
    /// Stored persistently in the system catalog.
    /// </summary>
    public class DatabaseInfo
    {
        /// <summary>
        /// The database name (unique within the root directory).
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// The absolute path to the database folder.
        /// </summary>
        public string Path { get; set; }

        /// <summary>
        /// The UTC timestamp when the database was created.
        /// </summary>
        public DateTime CreatedOn { get; set; }

        /// <summary>
        /// The last time this database was accessed (e.g. switched to).
        /// </summary>
        public DateTime LastAccessedOn { get; set; }

        /// <summary>
        /// Approximate size of the database folder on disk (in bytes).
        /// </summary>
        public long SizeInBytes { get; set; }

        /// <summary>
        /// Whether this database is currently the active one.
        /// </summary>
        [JsonIgnore] // We can ignore this in JSON since it’s runtime-only
        public bool IsActive { get; set; }

        // ------------------ Constructors ------------------

        public DatabaseInfo() { }

        public DatabaseInfo(string name, string path)
        {
            Name = name;
            Path = path;
            CreatedOn = DateTime.UtcNow;
            LastAccessedOn = DateTime.UtcNow;
            SizeInBytes = 0;
            IsActive = false;
        }

        // ------------------ Methods ------------------

        /// <summary>
        /// Updates the last-accessed timestamp to now.
        /// </summary>
        public void UpdateLastAccessed()
        {
            LastAccessedOn = DateTime.UtcNow;
        }

        /// <summary>
        /// Recalculates total folder size in bytes.
        /// </summary>
        public void RecalculateSize()
        {
            if (!Directory.Exists(Path))
            {
                SizeInBytes = 0;
                return;
            }

            long total = 0;
            foreach (string file in Directory.GetFiles(Path, "*", SearchOption.AllDirectories))
            {
                try
                {
                    total += new FileInfo(file).Length;
                }
                catch
                {
                    // Skip unreadable files
                }
            }

            SizeInBytes = total;
        }

        /// <summary>
        /// Returns a human-readable size string (e.g. "1.2 MB").
        /// </summary>
        public string GetSizeString()
        {
            double size = SizeInBytes;
            string[] units = { "B", "KB", "MB", "GB" };
            int unitIndex = 0;
            while (size >= 1024 && unitIndex < units.Length - 1)
            {
                size /= 1024;
                unitIndex++;
            }
            return $"{size:F1} {units[unitIndex]}";
        }
    }
}
