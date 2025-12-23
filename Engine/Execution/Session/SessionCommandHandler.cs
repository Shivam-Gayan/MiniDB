using DB.Engine.Database;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DB.Engine.Execution.Session
{
    /// <summary>
    /// Handles session-level (meta) commands like USE, SHOW DATABASES, SHOW TABLES, EXIT.
    /// These commands bypass the SQL parser.
    /// </summary>
    public static class SessionCommandHandler
    {
        public static bool TryHandle(
            string input,
            DatabaseManager dbManager,
            out bool shouldExit)
        {
            shouldExit = false;

            // Normalize
            var text = input.Trim().TrimEnd(';');

            if (text.Length == 0)
                return true;

            // EXIT / QUIT
            if (Equals(text, "exit") || Equals(text, "quit"))
            {
                shouldExit = true;
                return true;
            }

            // SHOW DATABASES
            if (Equals(text, "show databases"))
            {
                dbManager.ListDatabases(verbose: true);
                return true;
            }

            // USE dbname
            if (text.StartsWith("use ", StringComparison.OrdinalIgnoreCase))
            {
                var dbName = text.Substring(4).Trim();
                dbManager.UseDatabase(dbName);
                return true;
            }

            // SHOW TABLES
            if (Equals(text, "show tables"))
            {
                var ctx = dbManager.GetActiveContext();
                var tables = ctx.TableManager.ListTables();

                if (!tables.Any())
                {
                    Console.WriteLine("No tables found.");
                }
                else
                {
                    Console.WriteLine("\nTables:");
                    foreach (var t in tables)
                        Console.WriteLine($" - {t}");
                }
                return true;
            }

            // Not a session command
            return false;
        }

        private static bool Equals(string a, string b) =>
            a.Equals(b, StringComparison.OrdinalIgnoreCase);
    }
}
