using DB.Engine.Execution.Ast.DB.Engine.Execution.Ast;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DB.Engine.Execution.Ast
{
    /// <summary>
    /// Base class for top-level SQL statements.
    /// Example: SELECT, INSERT, CREATE INDEX
    /// </summary>
    public abstract class StatementNode : SqlNode
    {
    }
}
