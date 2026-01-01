using DB.Engine.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DB.Engine.Index
{
    public sealed class Key(FieldType type, object value) : IComparable<Key>
    {
        public FieldType Type { get; } = type;
        public object Value { get; } = value ?? throw new ArgumentNullException(nameof(value));


        /// <summary>
        /// Creates a new Key instance from the specified object, inferring the key type based on the object's runtime
        /// type.
        /// </summary>
        /// <param name="o">The object to convert to a Key. Supported types are int, string, bool, and double.</param>
        /// <returns>A Key representing the value and type of the specified object.</returns>
        /// <exception cref="NotSupportedException">Thrown if the type of o is not supported for key creation.</exception>
        public static Key FromObject(object o)
        {
            //already a Key
            if (o is Key k)
                return k;

            return o switch
            {
                int i => new Key(FieldType.Integer, i),
                string s => new Key(FieldType.String, s),
                bool b => new Key(FieldType.Boolean, b),
                double d => new Key(FieldType.Double, d),
                _ => throw new NotSupportedException(
                    $"Unsupported key type for indexing: {o.GetType().Name}")
            };
        }


        /// <summary>
        /// Main comparison logic so that the B+Tree can maintain sorted keys.
        /// </summary>
        public int CompareTo(Key? other)
        {
            if (other == null) return 1;

            // If types differ, sort by enum value
            if (Type != other.Type)
                return Type.CompareTo(other.Type);

            return Type switch
            {
                FieldType.Integer => ((int)Value).CompareTo((int)other.Value),
                FieldType.String => string.CompareOrdinal((string)Value, (string)other.Value),
                FieldType.Boolean => ((bool)Value).CompareTo((bool)other.Value),
                FieldType.Double => ((double)Value).CompareTo((double)other.Value),

                _ => throw new NotSupportedException($"Unsupported type: {Type}")
            };
        }

        public override string ToString() => Value.ToString()!;
    }
}
