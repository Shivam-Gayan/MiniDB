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
    /// This is the query planner 
    /// </summary>
    public static class CommandBuilder
    {
        public static ICommand Build(StatementNode statement)
        {
            return statement switch
            {
                CreateTableNode ct => BuildCreateTable(ct),
                CreateIndexNode ci => BuildCreateIndex(ci),
                InsertNode ins => BuildInsert(ins),
                SelectNode sel => BuildSelect(sel),
                DropTableNode dt => BuildDropTable(dt),
                DeleteNode del => BuildDelete(del),
                UpdateNode up => BuildUpdate(up),

                _ => throw new NotSupportedException(
                    $"Unsupported statement type: {statement.GetType().Name}")
            };
        }

        // ------------------- Builders -------------------

        private static ICommand BuildUpdate(UpdateNode node)
        {
            return new UpdateCommand(
                node.TableName,
                node.Column,
                node.Value,
                node.Where
            );
        }
        private static ICommand BuildDelete(DeleteNode node)
        {
            return new DeleteCommand(
                node.TableName,
                node.Where
            );
        }
        private static ICommand BuildDropTable(DropTableNode node)
        {
            return new DropTableCommand(node.TableName);
        }

        private static ICommand BuildCreateTable(CreateTableNode node)
        {
            return new CreateTableCommand(
                node.TableName,
                node.Columns
            );
        }

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
