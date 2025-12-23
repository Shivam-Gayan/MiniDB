using DB.Engine.Execution.Ast.Where;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DB.Engine.Execution.Ast
{
    public sealed class DeleteNode : StatementNode
    {
        public string Table { get; }
        public WhereNode? Where { get; }

        public DeleteNode(string table, WhereNode? where)
        {
            Table = table;
            Where = where;
        }
    }
}
