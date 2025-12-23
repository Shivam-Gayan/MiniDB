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

        public QueryResult Execute(DatabaseContext context)
        {
            var sw = System.Diagnostics.Stopwatch.StartNew();

            var rm = new RecordManager(context);
            var resultRows = new List<Dictionary<string, object>>();

            // -------- INDEX PATH --------
            if (_where is BinaryExpression expr &&
                context.IndexManager.HasIndex(_table, expr.Column))
            {
                switch (expr.Operator)
                {
                    case ComparisonOperator.Equal:
                        foreach (var rid in context.IndexManager.Search(_table, expr.Column, expr.Value))
                            AddIfMatch(rm.Read(_table, rid), resultRows);
                        break;

                    case ComparisonOperator.GreaterThan:
                        Range(context, rm, expr,
                            low: expr.Value, lowInc: false,
                            high: null, highInc: false,
                            resultRows);
                        break;

                    case ComparisonOperator.GreaterThanOrEqual:
                        Range(context, rm, expr,
                            low: expr.Value, lowInc: true,
                            high: null, highInc: false,
                            resultRows);
                        break;

                    case ComparisonOperator.LessThan:
                        Range(context, rm, expr,
                            low: null, lowInc: false,
                            high: expr.Value, highInc: false,
                            resultRows);
                        break;

                    case ComparisonOperator.LessThanOrEqual:
                        Range(context, rm, expr,
                            low: null, lowInc: false,
                            high: expr.Value, highInc: true,
                            resultRows);
                        break;

                    case ComparisonOperator.Between:
                        Range(context, rm, expr,
                            low: expr.Value, lowInc: true,
                            high: expr.SecondValue!, highInc: true,
                            resultRows);
                        break;
                }
            }
            // -------- TABLE SCAN FALLBACK --------
            else
            {
                var scan = new TableScan(context.TableManager, context.FileManager);
                foreach (var record in scan.ScanTable(_table))
                    AddIfMatch(record, resultRows);
            }

            sw.Stop();

            return new QueryResult(
                success: true,
                message: "Query executed successfully",
                rows: resultRows,
                execTime: sw.Elapsed.TotalMilliseconds
            );
        }

        private void AddIfMatch(Record? record, List<Dictionary<string, object>> rows)
        {
            if (record == null)
                return;

            if (!MatchesWhere(record))
                return;

            var row = new Dictionary<string, object>();
            for (int i = 0; i < record.Schema.Columns.Count; i++)
            {
                row[record.Schema.Columns[i]] = record.Values[i] ?? "NULL";
            }

            rows.Add(row);
        }


        private void Range(
            DatabaseContext context,
            RecordManager rm,
            BinaryExpression expr,
            object? low,
            bool lowInc,
            object? high,
            bool highInc,
            List<Dictionary<string,object>> rows)
        {
            foreach (var rid in context.IndexManager.RangeSearch(_table, expr.Column, low, lowInc, high, highInc))
            {
                AddIfMatch(rm.Read(_table, rid), rows);
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

            var rhs = Coerce(expr.Value!, columnType);
            int compare = Comparer<object>.Default.Compare(lhs!, rhs);

            return expr.Operator switch
            {
                ComparisonOperator.Equal => compare == 0,
                ComparisonOperator.GreaterThan => compare > 0,
                ComparisonOperator.GreaterThanOrEqual => compare >= 0,
                ComparisonOperator.LessThan => compare < 0,
                ComparisonOperator.LessThanOrEqual => compare <= 0,
                ComparisonOperator.Between =>
                    compare >= 0 &&
                    Comparer<object>.Default.Compare(lhs!, Coerce(expr.SecondValue!, columnType)) <= 0,
                _ => false
            };
        }


        private void PrintTable(List<Record> rows)
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
