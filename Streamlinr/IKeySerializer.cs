namespace Streamlinr;

/// <summary>
/// Serializes and deserializes Kafka message keys.
/// </summary>
/// <typeparam name="TKey">The key type handled by the serializer.</typeparam>
public interface IKeySerializer<TKey> {
    /// <summary>
    /// Serializes a key into Kafka message bytes.
    /// </summary>
    /// <param name="value">The key to serialize.</param>
    /// <param name="context">The serialization context.</param>
    /// <returns>The serialized bytes, or null for a null Kafka key.</returns>
    Byte[]? Serialize(TKey? value, SerializationContext context);

    /// <summary>
    /// Deserializes Kafka message bytes into a key.
    /// </summary>
    /// <param name="data">The bytes to deserialize, or null for a null Kafka key.</param>
    /// <param name="context">The serialization context.</param>
    /// <returns>The deserialized key.</returns>
    TKey? Deserialize(Byte[]? data, SerializationContext context);
}
