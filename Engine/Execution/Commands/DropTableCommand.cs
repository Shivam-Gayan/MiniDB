using DB.Engine.Database;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DB.Engine.Execution.Commands
{
    public sealed class DropTableCommand : ICommand
    {
        private readonly string _table;

        public DropTableCommand(string table)
        {
            _table = table;
        }

        public QueryResult Execute(DatabaseContext context)
        {
            var sw = Stopwatch.StartNew();

            context.TableManager.DropTable(_table);
            context.IndexManager.DropIndexesForTable(_table);

            sw.Stop();

            return new QueryResult(
                success: true,
                message: $"Table '{_table}' dropped",
                execTime: sw.Elapsed.TotalMilliseconds
            );
        }

    }
}
