using DB.Engine.Execution.Ast.Where;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DB.Engine.Execution.Ast
{
    /// <summary>
    /// AST node for SELECT statement.
    /// </summary>
    public sealed class SelectNode : StatementNode
    {
        public string TableName { get; }
        public WhereNode? WhereClause { get; }

        public SelectNode(string tableName, WhereNode? whereClause)
        {
            TableName = tableName;
            WhereClause = whereClause;
        }
    }
}
