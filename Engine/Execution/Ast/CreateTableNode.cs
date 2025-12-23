using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DB.Engine.Execution.Ast
{
    public sealed class CreateTableNode : StatementNode
    {
        public string TableName { get; }
        public IReadOnlyList<ColumnDefinition> Columns { get; }

        public CreateTableNode(string tableName, IReadOnlyList<ColumnDefinition> columns)
        {
            TableName = tableName;
            Columns = columns;
        }
    }

    public sealed class ColumnDefinition
    {
        public string Name { get; }
        public string Type { get; }

        public ColumnDefinition(string name, string type)
        {
            Name = name;
            Type = type;
        }
    }
}
