using DB.Engine.Database;
using DB.Engine.Execution.Ast.Where;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DB.Engine.Execution.Commands
{
    public sealed class SelectCommand : ICommand
    {
        private readonly string _table;
        private readonly WhereNode? _where;

        public SelectCommand(string table, WhereNode? where)
        {
            _table = table;
            _where = where;
        }

        public void Execute(DatabaseContext context)
        {
            var rm = new RecordManager(context);

            // For now, only support simple indexed WHERE
            if (_where is BinaryExpression expr &&
                expr.Operator == ComparisonOperator.Equal &&
                context.IndexManager.HasIndex(_table, expr.Column))
            {
                foreach (var rid in context.IndexManager.Search(_table, expr.Column, expr.Value))
                {
                    var record = rm.Read(_table, rid);
                    if (record != null)
                        Console.WriteLine(record);
                }
                return;
            }

            // Fallback (table scan)
            var scan = new TableScan(context.TableManager, context.FileManager);
            foreach (var record in scan.ScanTable(_table))
            {
                Console.WriteLine(record);
            }
        }
    }
}
