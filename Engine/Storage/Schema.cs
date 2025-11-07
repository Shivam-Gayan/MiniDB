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

        public Schema(string tableName, List<string> columns, List<FieldType> columnTypes)
        {

            if (columns.Count != columnTypes.Count)
            {
                throw new ArgumentException("Columns count must match ColumnTypes count.");
            }
            TableName = tableName;
            Columns = columns;
            ColumnTypes = columnTypes;

        }

        public int ColumnCount => Columns.Count;

    }
}
