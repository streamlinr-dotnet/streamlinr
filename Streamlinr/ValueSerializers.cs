namespace Streamlinr;

using System.Text;

/// <summary>
/// Built-in serializers for simple Kafka message value payloads.
/// </summary>
static public class ValueSerializers {
    /// <summary>
    /// Gets a UTF-8 string value serializer.
    /// </summary>
    static public IValueSerializer String { get; } = new StringValueSerializer();

    /// <summary>
    /// Gets a byte array value serializer.
    /// </summary>
    static public IValueSerializer Bytes { get; } = new BytesValueSerializer();

    sealed class StringValueSerializer : IValueSerializer {
        public Byte[] Serialize(Object value, Type valueType, SerializationContext context) {
            if (value is not String text)
                throw new InvalidOperationException($"Cannot serialize CLR type '{value.GetType().FullName}' with the string value serializer.");

            return Encoding.UTF8.GetBytes(text);
        }

        public Object Deserialize(Byte[] data, Type valueType, SerializationContext context) {
            if (valueType != typeof(String))
                throw new InvalidOperationException($"Cannot deserialize CLR type '{valueType.FullName}' with the string value serializer.");

            return Encoding.UTF8.GetString(data);
        }
    }

    sealed class BytesValueSerializer : IValueSerializer {
        public Byte[] Serialize(Object value, Type valueType, SerializationContext context) {
            if (value is not Byte[] data)
                throw new InvalidOperationException($"Cannot serialize CLR type '{value.GetType().FullName}' with the byte array value serializer.");

            return data;
        }

        public Object Deserialize(Byte[] data, Type valueType, SerializationContext context) {
            if (valueType != typeof(Byte[]))
                throw new InvalidOperationException($"Cannot deserialize CLR type '{valueType.FullName}' with the byte array value serializer.");

            return data;
        }
    }
}
