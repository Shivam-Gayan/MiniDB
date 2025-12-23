using DB.Engine.Database;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DB.Engine.Execution.Commands
{
    public sealed class InsertCommand : ICommand
    {
        private readonly string _table;
        private readonly object?[] _values;

        public InsertCommand(string table, object?[] values)
        {
            _table = table;
            _values = values;
        }

        public void Execute(DatabaseContext context)
        {
            var rm = new RecordManager(context);
            var record = rm.BuildRecord(_table, _values);
            rm.Insert(_table, record);
        }
    }
}
