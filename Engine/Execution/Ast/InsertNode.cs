using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DB.Engine.Execution.Ast
{
    /// <summary>
    /// AST node for INSERT INTO table VALUES (...)
    /// </summary>
    public sealed class InsertNode : StatementNode
    {
        public string TableName { get; }
        public IReadOnlyList<object?> Values { get; }

        public InsertNode(string tableName, IReadOnlyList<object?> values)
        {
            TableName = tableName;
            Values = values;
        }
    }
}
