using DB.Engine.Database; // for DatabaseContext

namespace DB.Engine.Execution
{
    /// <summary>
    /// Represents the per-session execution state.
    /// Holds the currently active DatabaseContext and session metadata.
    /// </summary>
    public class ExecutionContext
    {
        /// <summary>Currently active database runtime context (nullable).</summary>
        public DatabaseContext? ActiveDatabaseContext { get; private set; }

        /// <summary>Name of the currently active database (or null if none).</summary>
        public string? ActiveDatabaseName { get; private set; }

        /// <summary>Current user executing commands (simple identity for now).</summary>
        public string CurrentUser { get; private set; } = "root";

        /// <summary>Whether this session is in a transaction (placeholder for later).</summary>
        public bool InTransaction { get; private set; } = false;

        /// <summary>Last time the context was used or switched.</summary>
        public DateTime LastUsedUtc { get; private set; } = DateTime.UtcNow;

        public ExecutionContext() { }

        /// <summary>
        /// Attach a DatabaseContext to this execution session and mark it active.
        /// This method centralizes switching logic so the execution layer only
        /// needs to update this single object when the active DB changes.
        /// </summary>
        /// <param name="dbContext">The DatabaseContext to attach (must be non-null).</param>
        public void SetActiveDatabase(DatabaseContext dbContext)
        {
            ArgumentNullException.ThrowIfNull(dbContext);

            ActiveDatabaseContext = dbContext;
            ActiveDatabaseName = dbContext.DatabaseName;
            InTransaction = false;                 // new database → not in a transaction by default
            LastUsedUtc = DateTime.UtcNow;
        }

        /// <summary>
        /// Detach the current database from the session (useful on close).
        /// </summary>
        public void ClearActiveDatabase()
        {
            ActiveDatabaseContext = null;
            ActiveDatabaseName = null;
            InTransaction = false;
            LastUsedUtc = DateTime.UtcNow;
        }

        /// <summary>
        /// Convenience: returns true if an active DB is set.
        /// </summary>
        public bool HasActiveDatabase() => ActiveDatabaseContext != null;

        public DatabaseContext GetActiveDatabase(bool throwIfMissing = true)
        {
            if (ActiveDatabaseContext == null)
            {
                if (throwIfMissing) throw new InvalidOperationException("No active database set for this session.");
                return null!;
            }
            LastUsedUtc = DateTime.UtcNow;
            return ActiveDatabaseContext;
        }

        public bool TryGetActiveDatabase(out DatabaseContext? dbContext)
        {
            dbContext = ActiveDatabaseContext;
            if (dbContext != null) LastUsedUtc = DateTime.UtcNow;
            return dbContext != null;
        }
    }
}
