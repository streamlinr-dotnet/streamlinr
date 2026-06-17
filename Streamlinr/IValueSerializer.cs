namespace Streamlinr;

/// <summary>
/// Serializes and deserializes Kafka message values using an explicitly resolved CLR type.
/// </summary>
public interface IValueSerializer {
    /// <summary>
    /// Serializes a message value into Kafka message bytes.
    /// </summary>
    /// <param name="value">The value to serialize.</param>
    /// <param name="valueType">The CLR value type.</param>
    /// <param name="context">The serialization context.</param>
    /// <returns>The serialized bytes.</returns>
    Byte[] Serialize(Object value, Type valueType, SerializationContext context);

    /// <summary>
    /// Deserializes Kafka message bytes into a value of the resolved CLR type.
    /// </summary>
    /// <param name="data">The bytes to deserialize.</param>
    /// <param name="valueType">The resolved CLR value type.</param>
    /// <param name="context">The serialization context.</param>
    /// <returns>The deserialized value.</returns>
    Object Deserialize(Byte[] data, Type valueType, SerializationContext context);
}
