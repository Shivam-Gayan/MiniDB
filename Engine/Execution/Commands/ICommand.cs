using DB.Engine.Database;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DB.Engine.Execution.Commands
{
    /// <summary>
    /// Represents an executable database command.
    /// </summary>
    public interface ICommand
    {
        void Execute(DatabaseContext context);
    }
}
