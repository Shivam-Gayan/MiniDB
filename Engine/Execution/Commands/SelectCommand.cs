using DB.Engine.Database;
using DB.Engine.Execution.Ast.Where;
using DB.Engine.Storage;
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
            var rows = new List<Record>();

            // 1. Get Schema to determine correct types
            var schema = context.TableManager.GetSchema(_table);
            if (schema == null)
                throw new InvalidOperationException($"Table '{_table}' does not exist.");

            if (_where is BinaryExpression expr && context.IndexManager.HasIndex(_table, expr.Column))
            {
                var rm = new RecordManager(context);

                // 2. Find the column definition
                int colIndex = schema.Columns.IndexOf(expr.Column);
                if (colIndex < 0)
                    throw new InvalidOperationException($"Column '{expr.Column}' not found.");

                FieldType colType = schema.ColumnTypes[colIndex];

                // 3. COERCE (Convert) the query value to the correct Index Type
                // If we don't do this, we search for String "3" in an Integer Index, which fails.
                object val = Coerce(expr.Value!, colType);
                object? val2 = expr.SecondValue != null ? Coerce(expr.SecondValue, colType) : null;

                switch (expr.Operator)
                {
                    case ComparisonOperator.Equal:
                        foreach (var rid in context.IndexManager.Search(_table, expr.Column, val))
                        {
                            var record = rm.Read(_table, rid);
                            if (record != null && MatchesWhere(record))
                                rows.Add(record);
                        }
                        PrintTable(rows);
                        return;

                    case ComparisonOperator.GreaterThan:
                    case ComparisonOperator.GreaterThanOrEqual:
                        // Pass coerced 'val', not 'expr.Value'
                        Range(context, rm, expr, low: val, high: null, rows);
                        PrintTable(rows);
                        return;

                    case ComparisonOperator.LessThan:
                    case ComparisonOperator.LessThanOrEqual:
                        // Pass coerced 'val'
                        Range(context, rm, expr, low: null, high: val, rows);
                        PrintTable(rows);
                        return;

                    case ComparisonOperator.Between:
                        // Pass coerced 'val' and 'val2'
                        Range(context, rm, expr, low: val, high: val2, rows);
                        PrintTable(rows);
                        return;
                }
            }


            // Fallback (table scan)
            var scan = new TableScan(context.TableManager, context.FileManager);
            foreach (var record in scan.ScanTable(_table))
            {
                if (MatchesWhere(record))
                    rows.Add(record);
            }
            PrintTable(rows);

        }

        private void Range(DatabaseContext context, RecordManager rm, BinaryExpression expr, object? low, object? high, List<Record> rows)
        {
            foreach (var (_, rid) in context.IndexManager.RangeScan(_table, expr.Column, low, high))
            {
                var record = rm.Read(_table, rid);
                // Double check MatchesWhere to handle complex logic not covered by index
                if (record != null && MatchesWhere(record))
                    rows.Add(record);
            }
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

            // Coerce RHS to match LHS type for valid comparison
            var rhs = Coerce(expr.Value!, columnType);

            // Handle NULLs safely
            if (lhs == null || rhs == null)
                return false;

            int compare = Comparer<object>.Default.Compare(lhs, rhs);

            return expr.Operator switch
            {
                ComparisonOperator.Equal => compare == 0,
                ComparisonOperator.GreaterThan => compare > 0,
                ComparisonOperator.GreaterThanOrEqual => compare >= 0,
                ComparisonOperator.LessThan => compare < 0,
                ComparisonOperator.LessThanOrEqual => compare <= 0,
                ComparisonOperator.Between =>
                    compare >= 0 &&
                    Comparer<object>.Default.Compare(lhs, Coerce(expr.SecondValue!, columnType)) <= 0,
                _ => false
            };
        }

        private static void PrintTable(List<Record> rows)
        {
            if (rows.Count == 0)
            {
                Console.WriteLine("Empty set");
                return;
            }

            var schema = rows[0].Schema;
            int cols = schema.Columns.Count;
            int[] widths = new int[cols];

            for (int i = 0; i < cols; i++)
                widths[i] = schema.Columns[i].Length;

            foreach (var r in rows)
                for (int i = 0; i < cols; i++)
                    widths[i] = Math.Max(widths[i], (r.Values[i]?.ToString() ?? "NULL").Length);

            void Line()
            {
                Console.Write("+");
                for (int i = 0; i < cols; i++)
                    Console.Write(new string('-', widths[i] + 2) + "+");
                Console.WriteLine();
            }

            Line();
            Console.Write("|");
            for (int i = 0; i < cols; i++)
                Console.Write($" {schema.Columns[i].PadRight(widths[i])} |");
            Console.WriteLine();
            Line();

            foreach (var r in rows)
            {
                Console.Write("|");
                for (int i = 0; i < cols; i++)
                    Console.Write($" {(r.Values[i]?.ToString() ?? "NULL").PadRight(widths[i])} |");
                Console.WriteLine();
            }

            Line();
            Console.WriteLine($"{rows.Count} rows in set");
        }

        private object Coerce(object value, FieldType type)
        {
            try
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
            catch
            {
                // If conversion fails, return original value and let comparison fail gracefully later
                return value;
            }
        }
    }
}