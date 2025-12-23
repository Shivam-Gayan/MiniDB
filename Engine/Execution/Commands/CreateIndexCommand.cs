using DB.Engine.Database;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DB.Engine.Execution.Commands
{
    public sealed class CreateIndexCommand : ICommand
    {
        private readonly string _table;
        private readonly string _column;

        public CreateIndexCommand(string table, string column)
        {
            _table = table;
            _column = column;
        }

        public void Execute(DatabaseContext context)
        {
            context.IndexManager.CreateIndex(_table, _column);
        }
    }
}
