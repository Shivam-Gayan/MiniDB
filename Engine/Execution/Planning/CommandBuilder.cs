using DB.Engine.Execution.Ast;
using DB.Engine.Execution.Commands;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DB.Engine.Execution.Planning
{
    /// <summary>
    /// Converts AST nodes into executable Commands.
    /// This is the query planner (very simple version).
    /// </summary>
    public static class CommandBuilder
    {
        public static ICommand Build(StatementNode statement)
        {
            return statement switch
            {
                CreateIndexNode createIndex => BuildCreateIndex(createIndex),
                InsertNode insert => BuildInsert(insert),
                SelectNode select => BuildSelect(select),
                _ => throw new NotSupportedException(
                    $"Unsupported statement type: {statement.GetType().Name}")
            };
        }

        // ------------------- Builders -------------------

        private static ICommand BuildCreateIndex(CreateIndexNode node)
        {
            return new CreateIndexCommand(
                node.TableName,
                node.ColumnName
            );
        }

        private static ICommand BuildInsert(InsertNode node)
        {
            return new InsertCommand(
                node.TableName,
                node.Values.ToArray()
            );
        }

        private static ICommand BuildSelect(SelectNode node)
        {
            // We pass WHERE as-is.
            // Execution layer will decide index vs scan.
            return new SelectCommand(
                node.TableName,
                node.WhereClause
            );
        }
    }
}
