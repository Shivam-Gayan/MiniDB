

namespace DB.Engine.Storage
{
    public static class DbOptions
    {
        public const int PageSize = 4096; // 4 KB
        public const int HeaderSize = 32; // bytes reserved for page header
        public const string DefaultDbFile = "database.db";

    }
}
