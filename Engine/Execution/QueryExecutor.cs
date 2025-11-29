using DB.Engine.Database;
using DB.Engine.Storage;

namespace DB.Engine.Execution
{
    public class QueryExecutor
    {
        private readonly ExecutionContext _execCtx;

        public QueryExecutor(ExecutionContext execCtx)
        {
            _execCtx = execCtx ?? throw new ArgumentNullException(nameof(execCtx));
        }

        private DatabaseContext GetCtx() => _execCtx.GetActiveDatabase(true);

        public QueryResult CreateTable(string tableName, Schema schema)
        {
            if (string.IsNullOrWhiteSpace(tableName)) return QueryResult.Fail("Table name cannot be empty.");
            if (schema == null) return QueryResult.Fail("Schema cannot be null.");

            try
            {
                var ctx = GetCtx();

                if (ctx.TableManager.TableExists(tableName))
                    return QueryResult.Fail($"Table '{tableName}' already exists in database '{ctx.DatabaseName}'.");

                ctx.TableManager.CreateTable(tableName, schema);
                return QueryResult.Ok($"Table '{tableName}' created successfully.");
            }
            catch (Exception ex)
            {
                return QueryResult.Fail(ex.Message);
            }
        }

        public RID Insert(string tableName, IList<object?> values)
        {
            var ctx = GetCtx();
            var rm = new RecordManager(ctx);
            var record = rm.BuildRecord(tableName, values);
            return rm.Insert(tableName, record);
        }

        public IEnumerable<Record> SelectAll(string tableName)
        {
            var ctx = GetCtx();
            var scan = new TableScan(ctx);
            return scan.ScanTable(tableName);
        }

        public bool Delete(string tableName, RID rid)
        {
            var ctx = GetCtx();
            var rm = new RecordManager(ctx);
            return rm.Delete(tableName, rid);
        }
    }
}
