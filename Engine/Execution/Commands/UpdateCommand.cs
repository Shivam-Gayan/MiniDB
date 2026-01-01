using DB.Engine.Database;
using DB.Engine.Execution.Ast;
using DB.Engine.Execution.Ast.Where;
using DB.Engine.Storage;

namespace DB.Engine.Execution.Commands
{
    public class UpdateCommand : ICommand
    {
        private readonly string _tableName;
        private readonly string _column;
        private readonly object _newValue;
        private readonly WhereNode? _where;

        public UpdateCommand(string tableName, string column, object newValue, WhereNode? where)
        {
            _tableName = tableName;
            _column = column;
            _newValue = newValue;
            _where = where;
        }

        public void Execute(DatabaseContext context)
        {
            var rm = new RecordManager(context);
            var scan = new TableScan(context);
            var schema = context.TableManager.GetSchema(_tableName);

            if (schema == null)
                throw new Exception($"Table '{_tableName}' not found.");

            int colIndex = schema.Columns.IndexOf(_column);
            if (colIndex < 0)
                throw new Exception($"Column '{_column}' does not exist in '{_tableName}'.");

            // 1. Identify rows to update
            var rowsToUpdate = new List<(RID rid, Record record)>();

            foreach (var (record, rid) in scan.Scan(_tableName))
            {
                if (MatchesWhere(record, _where))
                {
                    rowsToUpdate.Add((rid, record));
                }
            }

            int count = 0;

            // 2. Perform Update (Delete + Insert)
            foreach (var (rid, oldRecord) in rowsToUpdate)
            {
                // a. Create new values array
                var newValues = oldRecord.Values.ToArray();

                // b. Apply update (Coerce ensures type safety)
                newValues[colIndex] = Coerce(_newValue, schema.ColumnTypes[colIndex]);

                // c. Delete old record (removes from index)
                rm.Delete(_tableName, rid);

                // d. Insert new record (adds to index)
                try
                {
                    var newRecord = new Record(schema, newValues);
                    rm.Insert(_tableName, newRecord);
                    count++;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error updating row: {ex.Message}");
                    // Ideally rollback delete here, but simpler to abort for now
                }
            }

            Console.WriteLine($"Updated {count} rows.");
        }

        // Helper: Logic reused from DeleteCommand/SelectCommand
        private bool MatchesWhere(Record record, WhereNode? where)
        {
            if (where == null) return true;
            if (where is BinaryExpression expr)
            {
                int colIndex = record.Schema.Columns.IndexOf(expr.Column);
                if (colIndex < 0) return false;
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
                _ => throw new Exception($"Type mismatch")
            };
        }
    }
}