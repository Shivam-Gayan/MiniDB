using System.Text;

namespace DB.Engine.Storage
{
    public class Record
    {
        public Schema Schema { get; }
        public object[] Values { get; }

        public Record(Schema schema, params object[] values)
        {
            if (schema.ColumnCount != values.Length)
                throw new ArgumentException("Value count does not match schema.");

            Schema = schema;
            Values = values;
        }

        public byte[] ToBytes()
        {
            using var ms = new MemoryStream();
            using var writer = new BinaryWriter(ms);

            for (int i = 0; i < Schema.ColumnCount; i++)
            {
                var type = Schema.ColumnTypes[i];
                var value = Values[i];

                switch (type)
                {
                    case FieldType.Integer:
                        writer.Write(Convert.ToInt32(value));
                        break;
                    case FieldType.Double:
                        writer.Write(Convert.ToDouble(value));
                        break;
                    case FieldType.Boolean:
                        writer.Write((bool)value);
                        break;
                    case FieldType.String:
                        var str = value?.ToString() ?? string.Empty;
                        var bytes = Encoding.UTF8.GetBytes(str);
                        writer.Write((short)bytes.Length);
                        writer.Write(bytes);
                        break;
                    default:
                        throw new InvalidOperationException($"Unsupported field type: {type}");
                }
            }

            return ms.ToArray();
        }

        public static Record FromBytes(Schema schema, byte[] data)
        {
            using var ms = new MemoryStream(data);
            using var reader = new BinaryReader(ms);

            object[] values = new object[schema.ColumnCount];

            for (int i = 0; i < schema.ColumnCount; i++)
            {
                var type = schema.ColumnTypes[i];

                switch (type)
                {
                    case FieldType.Integer:
                        values[i] = reader.ReadInt32();
                        break;
                    case FieldType.Double:
                        values[i] = reader.ReadDouble();
                        break;
                    case FieldType.Boolean:
                        values[i] = reader.ReadBoolean();
                        break;
                    case FieldType.String:
                        short len = reader.ReadInt16();
                        byte[] bytes = reader.ReadBytes(len);
                        values[i] = Encoding.UTF8.GetString(bytes);
                        break;
                }
            }

            return new Record(schema, values);
        }

        public override string ToString()
        {
            var parts = new List<string>();
            for (int i = 0; i < Schema.ColumnCount; i++)
                parts.Add($"{Schema.Columns[i]}={Values[i]}");

            return $"[{string.Join(", ", parts)}]";
        }
    }
}
