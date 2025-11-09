using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DB.Engine.Storage
{
    public class Schema
    {
        public string TableName { get; }
        public List<string> Columns { get; }
        public List<FieldType> ColumnTypes { get; }
        public List<bool> IsNullable { get; }

        public Schema(string tableName, List<string> columns, List<FieldType> columnTypes, List<bool>? isNullable = null)
        {

            if (columns.Count != columnTypes.Count)
            {
                throw new ArgumentException("Columns count must match ColumnTypes count.");
            }
            TableName = tableName;
            Columns = columns;
            ColumnTypes = columnTypes;

            // If nullability list is not provided, assume all NOT NULL by default
            IsNullable = isNullable switch
            {
                null => [.. Enumerable.Repeat(false, columns.Count)],
                _ when isNullable.Count == columns.Count => isNullable,
                _ => throw new ArgumentException("isNullable must match columns length")
            };

        }

        public int ColumnCount => Columns.Count;

    }
}
