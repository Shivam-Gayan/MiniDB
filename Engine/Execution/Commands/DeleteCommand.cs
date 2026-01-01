using DB.Engine.Database;
using DB.Engine.Execution.Ast.Where;
using DB.Engine.Storage;

namespace DB.Engine.Execution.Commands
{
    public class DeleteCommand : ICommand
    {
        private readonly string _tableName;
        private readonly WhereNode? _where;

        public DeleteCommand(string tableName, WhereNode? where)
        {
            _tableName = tableName;
            _where = where;
        }

        public void Execute(DatabaseContext context)
        {
            var rm = new RecordManager(context);
            var scan = new TableScan(context);
            int count = 0;

            // 1. Identify rows to delete
            var rowsToDelete = new List<RID>();

            foreach (var (record, rid) in scan.Scan(_tableName))
            {
                if (MatchesWhere(record, _where))
                {
                    rowsToDelete.Add(rid);
                }
            }

            // 2. Perform deletion
            foreach (var rid in rowsToDelete)
            {
                if (rm.Delete(_tableName, rid))
                {
                    count++;
                }
            }

            Console.WriteLine($"Deleted {count} rows.");
        }

        private bool MatchesWhere(Record record, WhereNode? where)
        {
            if (where == null) return true;

            if (where is BinaryExpression expr)
            {
                int colIndex = record.Schema.Columns.IndexOf(expr.Column);
                if (colIndex < 0) throw new InvalidOperationException($"Column {expr.Column} not found");

                var val = record.Values[colIndex];
                var target = Coerce(expr.Value!, record.Schema.ColumnTypes[colIndex]);

                int cmp = Comparer<object>.Default.Compare(val!, target);

                return expr.Operator switch
                {
                    ComparisonOperator.Equal => cmp == 0,
                    ComparisonOperator.GreaterThan => cmp > 0,
                    ComparisonOperator.LessThan => cmp < 0,
                    ComparisonOperator.GreaterThanOrEqual => cmp >= 0,
                    ComparisonOperator.LessThanOrEqual => cmp <= 0,
                    _ => false
                };
            }
            return true;
        }

        private object Coerce(object value, FieldType type)
        {
            return type switch
            {
                FieldType.Integer => Convert.ToInt32(value),
                FieldType.Double => Convert.ToDouble(value),
                FieldType.Boolean => Convert.ToBoolean(value),
                FieldType.String => value.ToString()!,
                _ => throw new InvalidOperationException($"Unsupported type {type}")
            };
        }
    }
}