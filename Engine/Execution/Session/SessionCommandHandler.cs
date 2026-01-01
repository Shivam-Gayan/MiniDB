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
        public static bool TryHandle(string input,DatabaseManager dbManager,out bool shouldExit)
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
            // SHOW INDEXES
            if (Equals(text, "show indexes"))
            {
                var ctx = dbManager.GetActiveContext();
                var indexes = ctx.IndexManager.ListIndexes();

                if (!indexes.Any())
                {
                    Console.WriteLine("No indexes found.");
                }
                else
                {
                    Console.WriteLine("\nIndexes:");
                    foreach (var (table, column) in indexes)
                    {
                        Console.WriteLine($" - {table}({column})");
                    }
                }

                return true;
            }
            // CREATE DATABASE dbname
            if (text.StartsWith("create database ", StringComparison.OrdinalIgnoreCase))
            {
                var dbName = text.Substring("create database ".Length).Trim();

                if (string.IsNullOrWhiteSpace(dbName))
                {
                    Console.WriteLine("Database name cannot be empty.");
                    return true;
                }

                try
                {
                    dbManager.CreateDatabase(dbName);
                    Console.WriteLine($"Database '{dbName}' created.");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error: {ex.Message}");
                }

                return true;
            }
            // DROP DATABASE dbname
            if (text.StartsWith("drop database ", StringComparison.OrdinalIgnoreCase))
            {
                var dbName = text.Substring("drop database ".Length).Trim();

                if (string.IsNullOrWhiteSpace(dbName))
                {
                    Console.WriteLine("Database name cannot be empty.");
                    return true;
                }

                try
                {
                    dbManager.DropDatabase(dbName);
                    Console.WriteLine($"Database '{dbName}' dropped.");

                    // If active DB was dropped, no DB is active now
                    if (dbManager.ActiveDatabase == dbName)
                    {
                        Console.WriteLine("No database selected.");
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error: {ex.Message}");
                }

                return true;
            }
            // DROP INDEX table.column
            if (text.StartsWith("drop index ", StringComparison.OrdinalIgnoreCase))
            {
                var target = text.Substring("drop index ".Length).Trim();
                var parts = target.Split('.', 2);

                if (parts.Length != 2)
                {
                    Console.WriteLine("Usage: DROP INDEX table.column");
                    return true;
                }

                try
                {
                    var ctx = dbManager.GetActiveContext();
                    ctx.IndexManager.DropIndex(parts[0], parts[1]);
                    Console.WriteLine($"Index {parts[0]}({parts[1]}) dropped.");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error: {ex.Message}");
                }

                return true;
            }
            // VACUUM (Reclaim space in the current database)
            if (Equals(text, "vacuum"))
            {
                try
                {
                    // Get the context for the currently selected database (e.g., 'testdb')
                    var ctx = dbManager.GetActiveContext();

                    // Trigger the VacuumAll method implemented in TableManager
                    ctx.TableManager.VacuumAll();
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error: {ex.Message}");
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
