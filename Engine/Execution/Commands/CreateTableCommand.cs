using DB.Engine.Database;
using DB.Engine.Execution.Ast;
using DB.Engine.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DB.Engine.Execution.Commands
{
    public sealed class CreateTableCommand : ICommand
    {
        private readonly string _tableName;
        private readonly IReadOnlyList<ColumnDefinition> _columns;

        public CreateTableCommand(string tableName, IReadOnlyList<ColumnDefinition> columns)
        {
            _tableName = tableName;
            _columns = columns;
        }

        public void Execute(DatabaseContext context)
        {
            var columnNames = new List<string>();
            var columnTypes = new List<FieldType>();
            var nullable = new List<bool>();

            foreach (var col in _columns)
            {
                columnNames.Add(col.Name);
                columnTypes.Add(ParseType(col.Type));
                nullable.Add(true); // default nullable for now
            }

            var schema = new Schema(
                _tableName,
                columnNames,
                columnTypes,
                nullable
            );

            context.TableManager.CreateTable(_tableName, schema);

            Console.WriteLine($"Table '{_tableName}' created.");
        }

        private static FieldType ParseType(string type)
        {
            return type.ToUpperInvariant() switch
            {
                "INT" or "INTEGER" => FieldType.Integer,
                "STRING" or "TEXT" => FieldType.String,
                "BOOL" or "BOOLEAN" => FieldType.Boolean,
                "DOUBLE" or "FLOAT" => FieldType.Double,
                _ => throw new InvalidOperationException($"Unknown column type '{type}'")
            };
        }
    }
}
