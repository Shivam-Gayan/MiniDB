using DB.Engine.Database;
using DB.Engine.Execution.Ast.Where;
using DB.Engine.Storage;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DB.Engine.Execution.Commands
{
    public sealed class DeleteCommand : ICommand
    {
        private readonly string _table;
        private readonly WhereNode? _where;

        public DeleteCommand(string table, WhereNode? where)
        {
            _table = table;
            _where = where;
        }

        public QueryResult Execute(DatabaseContext context)
        {
            var sw = Stopwatch.StartNew();
            var rm = new RecordManager(context);
            int deleted = 0;

            var scan = new TableScan(context.TableManager, context.FileManager);

            foreach (var record in scan.ScanTable(_table))
            {
                if (!MatchesWhere(record))
                    continue;

                var rid = record.Rid; // assumes Record carries RID
                if (rm.Delete(_table, rid))
                    deleted++;
            }

            sw.Stop();

            return new QueryResult(
                success: true,
                message: $"{deleted} rows deleted",
                execTime: sw.Elapsed.TotalMilliseconds
            );
        }

        private bool MatchesWhere(Record record)
        {
            if (_where == null)
                return true;

            if (_where is not BinaryExpression expr)
                return true;

            int colIndex = record.Schema.Columns.IndexOf(expr.Column);
            if (colIndex < 0)
                return false;

            var columnType = record.Schema.ColumnTypes[colIndex];
            var lhs = record.Values[colIndex];
            var rhs = Coerce(expr.Value!, columnType);

            int cmp = Comparer<object>.Default.Compare(lhs!, rhs);

            return expr.Operator switch
            {
                ComparisonOperator.Equal => cmp == 0,
                ComparisonOperator.GreaterThan => cmp > 0,
                ComparisonOperator.GreaterThanOrEqual => cmp >= 0,
                ComparisonOperator.LessThan => cmp < 0,
                ComparisonOperator.LessThanOrEqual => cmp <= 0,
                _ => false
            };
        }

        private object Coerce(object value, FieldType type)
        {
            return type switch
            {
                FieldType.Integer => Convert.ToInt32(value),
                FieldType.Double => Convert.ToDouble(value),
                FieldType.Boolean => Convert.ToBoolean(value),
                FieldType.String => value.ToString()!,
                _ => throw new InvalidOperationException()
            };
        }
    }
}
