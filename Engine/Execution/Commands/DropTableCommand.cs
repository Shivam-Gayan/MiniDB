using DB.Engine.Database;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DB.Engine.Execution.Commands
{
    public sealed class DropTableCommand : ICommand
    {
        private readonly string _table;

        public DropTableCommand(string table)
        {
            _table = table;
        }

        public void Execute(DatabaseContext context)
        {
            context.TableManager.DropTable(_table);
            context.IndexManager.DropIndexesForTable(_table);

            Console.WriteLine($"Table '{_table}' dropped.");
        }
    }
}
