using System.Text;

namespace DB.Engine.Storage
{
    public sealed class Record
    {
        public Schema Schema { get; }
        public object?[] Values { get; }
        public RID Rid { get; }

        // ---------------- CONSTRUCTOR ----------------

        public Record(Schema schema, RID rid, params object?[] values)
        {
            if (schema.ColumnCount != values.Length)
                throw new ArgumentException("Value count does not match schema.");

            // Validate types
            for (int i = 0; i < schema.ColumnCount; i++)
            {
                if (values[i] == null && !schema.IsNullable[i])
                {
                    throw new InvalidOperationException(
                        $"Column '{schema.Columns[i]}' cannot be NULL.");
                }

                var expected = schema.ColumnTypes[i];
                var value = values[i];
                var nullable = schema.IsNullable[i];
                var colName = schema.Columns[i];

                if (value == null)
                {
                    if (!nullable)
                        throw new ArgumentException($"Field '{colName}' cannot be NULL.");
                    continue;
                }

                switch (expected)
                {
                    case FieldType.Integer when value is not int:
                        throw new ArgumentException($"Field '{colName}' expects INTEGER.");
                    case FieldType.String when value is not string:
                        throw new ArgumentException($"Field '{colName}' expects STRING.");
                    case FieldType.Boolean when value is not bool:
                        throw new ArgumentException($"Field '{colName}' expects BOOLEAN.");
                    case FieldType.Double when value is not double:
                        throw new ArgumentException($"Field '{colName}' expects DOUBLE.");
                }
            }

            Schema = schema;
            Values = values;
            Rid = rid;
        }

        // ---------------- SERIALIZATION ----------------
        public static byte[] Serialize(Schema schema, object?[] values)
        {
            using var ms = new MemoryStream();
            using var writer = new BinaryWriter(ms);

            for (int i = 0; i < schema.ColumnCount; i++)
            {
                var type = schema.ColumnTypes[i];
                var value = values[i];

                switch (type)
                {
                    case FieldType.Integer:
                        writer.Write((int)value!);
                        break;
                    case FieldType.Double:
                        writer.Write((double)value!);
                        break;
                    case FieldType.Boolean:
                        writer.Write((bool)value!);
                        break;
                    case FieldType.String:
                        var str = (string?)value ?? string.Empty;
                        var bytes = Encoding.UTF8.GetBytes(str);
                        writer.Write((short)bytes.Length);
                        writer.Write(bytes);
                        break;
                }
            }

            return ms.ToArray();
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
                        writer.Write((int)value!);
                        break;

                    case FieldType.Double:
                        writer.Write((double)value!);
                        break;

                    case FieldType.Boolean:
                        writer.Write((bool)value!);
                        break;

                    case FieldType.String:
                        var str = (string?)value ?? string.Empty;
                        var bytes = Encoding.UTF8.GetBytes(str);
                        writer.Write((short)bytes.Length);
                        writer.Write(bytes);
                        break;

                    default:
                        throw new InvalidOperationException($"Unsupported field type {type}");
                }
            }

            return ms.ToArray();
        }

        // ---------------- DESERIALIZATION ----------------

        public static Record FromBytes(
            Schema schema,
            byte[] data,
            RID rid)
        {
            using var ms = new MemoryStream(data);
            using var reader = new BinaryReader(ms);

            var values = new object?[schema.ColumnCount];

            for (int i = 0; i < schema.ColumnCount; i++)
            {
                var type = schema.ColumnTypes[i];

                values[i] = type switch
                {
                    FieldType.Integer => reader.ReadInt32(),
                    FieldType.Double => reader.ReadDouble(),
                    FieldType.Boolean => reader.ReadBoolean(),
                    FieldType.String => Encoding.UTF8.GetString(reader.ReadBytes(reader.ReadInt16())),
                    _ => throw new InvalidOperationException($"Unsupported field type {type}")
                };
            }

            return new Record(schema, rid, values);
        }

        // ---------------- DEBUG ----------------

        public override string ToString()
        {
            var parts = new List<string>();
            for (int i = 0; i < Schema.ColumnCount; i++)
                parts.Add($"{Schema.Columns[i]}={Values[i]}");

            return $"[{string.Join(", ", parts)}]";
        }
    }
}
