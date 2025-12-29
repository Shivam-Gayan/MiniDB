using DB.Engine.Database;
using DB.Engine.Execution.Parsing;
using DB.Engine.Execution.Planning;
using DB.Engine.Execution.Session;

class Program
{
    static void Main()
    {
        Console.WriteLine("SimpleDB Engine");
        Console.WriteLine("Type EXIT to quit\n");

        // ------------------- Bootstrap -------------------

        var dbManager = new DatabaseManager();

        // Ensure default database exists
        const string defaultDb = "testdb";

        if (!dbManager.ListDatabases(false).Contains(defaultDb))
        {
            dbManager.CreateDatabase(defaultDb);
        }

        dbManager.UseDatabase(defaultDb);

        // ------------------- REPL -------------------

        while (true)
        {
            // Prompt: show db name if active
            string prompt = dbManager.ActiveDatabase != null
                ? $"{dbManager.ActiveDatabase}> "
                : "db> ";

            Console.Write(prompt);

            var input = Console.ReadLine();
            if (input == null)
                continue;

            input = input.Trim();

            if (input.Length == 0)
                continue;

            try
            {
                // 1️ Session / Meta commands (USE, SHOW, EXIT)
                if (SessionCommandHandler.TryHandle(input, dbManager, out bool shouldExit))
                {
                    if (shouldExit)
                        break;

                    continue;
                }

                // 2️ SQL pipeline
                var lexer = new Lexer(input);
                var tokens = lexer.Tokenize();

                var parser = new SqlParser(tokens);
                var ast = parser.ParseStatement();

                var command = CommandBuilder.Build(ast);
                command.Execute(dbManager.GetActiveContext());
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
            }
        }

        Console.WriteLine("Bye.");
    }
}
