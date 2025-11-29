namespace DB.Engine.Execution
{
    /// <summary>
    /// Represents the outcome of a query or command execution.
    /// </summary>
    public class QueryResult
    {
        public bool Success { get; }
        public string Message { get; }
        public List<Dictionary<string, object>>? Rows { get; }
        public double ExecutionTimeMs { get; }

        public QueryResult(bool success, string message, List<Dictionary<string, object>>? rows = null, double execTime = 0)
        {
            Success = success;
            Message = message;
            Rows = rows;
            ExecutionTimeMs = execTime;
        }

        // ------------------ Factory Helpers ------------------

        public static QueryResult Ok(string message = "Success")
            => new QueryResult(true, message);

        public static QueryResult Fail(string message)
            => new QueryResult(false, message);

        public static QueryResult WithData(List<Dictionary<string, object>> rows, string message = "Query executed successfully")
            => new QueryResult(true, message, rows);

        // ------------------ Presentation ------------------

        public void Print()
        {
            if (!Success)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"{Message}");
                Console.ResetColor();
                return;
            }

            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{Message}");
            Console.ResetColor();

            if (Rows != null && Rows.Count > 0)
            {
                Console.WriteLine("\nResults:\n");
                PrintTable(Rows);
            }

            if (ExecutionTimeMs > 0)
                Console.WriteLine($"\nTime: {ExecutionTimeMs:F2} ms");
        }

        private static void PrintTable(List<Dictionary<string, object>> rows)
        {

            var columns = rows[0].Keys.ToList();

            // Print header
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine(string.Join(" | ", columns));
            Console.WriteLine(new string('-', columns.Sum(c => c.Length + 3)));
            Console.ResetColor();

            // Print rows
            foreach (var row in rows)
            {
                Console.WriteLine(string.Join(" | ", columns.Select(c => row[c])));
            }

        }
    }
}
