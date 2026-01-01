using DB.Engine.Execution.Ast.Where;

namespace DB.Engine.Execution.Ast
{
    public class UpdateNode : StatementNode
    {
        public string TableName { get; }
        public string Column { get; }
        public object Value { get; }
        public WhereNode? Where { get; }

        public UpdateNode(string tableName, string column, object value, WhereNode? where)
        {
            TableName = tableName;
            Column = column;
            Value = value;
            Where = where;
        }
    }
}