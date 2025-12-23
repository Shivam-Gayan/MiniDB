using DB.Engine.Database;
using DB.Engine.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DB.Engine.Execution.Commands
{
    public sealed class InsertCommand : ICommand
    {
        private readonly string _table;
        private readonly object?[] _values;

        public InsertCommand(string table, object?[] values)
        {
            _table = table;
            _values = values;
        }

        public QueryResult Execute(DatabaseContext context)
        {
            var sw = System.Diagnostics.Stopwatch.StartNew();
            var rm = new RecordManager(context);
            var schema = context.TableManager.GetSchema(_table) ?? throw new InvalidOperationException($"Table '{_table}' does not exist.");

            var values = rm.BuildRecordValues(_table, _values);

            // Insert returns RID
            var rid = context.TableManager.Insert(_table, schema, values);

            // Materialize record (optional return)
            var record = new Record(schema, rid, values);

            sw.Stop();
            return new QueryResult(
                success: true,
                message: "1 row inserted",
                execTime: sw.Elapsed.TotalMilliseconds
            );
        }
    }
}
