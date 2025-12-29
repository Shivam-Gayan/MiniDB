using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DB.Engine.Execution.Ast
{
    public sealed class DropTableNode : StatementNode
    {
        public string TableName { get; }

        public DropTableNode(string tableName)
        {
            TableName = tableName;
        }
    }
}
