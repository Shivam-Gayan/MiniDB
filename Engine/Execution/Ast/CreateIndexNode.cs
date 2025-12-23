using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DB.Engine.Execution.Ast
{
    /// <summary>
    /// AST node for CREATE INDEX statement.
    /// </summary>
    public sealed class CreateIndexNode : StatementNode
    {
        public string IndexName { get; }
        public string TableName { get; }
        public string ColumnName { get; }

        public CreateIndexNode(string indexName, string tableName, string columnName)
        {
            IndexName = indexName;
            TableName = tableName;
            ColumnName = columnName;
        }
    }
}
