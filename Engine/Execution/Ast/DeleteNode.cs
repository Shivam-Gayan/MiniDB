using DB.Engine.Execution.Ast.Where;

namespace DB.Engine.Execution.Ast
{
    public class DeleteNode : StatementNode
    {
        public string TableName { get; }
        public WhereNode? Where { get; }

        public DeleteNode(string tableName, WhereNode? where)
        {
            TableName = tableName;
            Where = where;
        }
    }
}