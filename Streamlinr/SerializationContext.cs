namespace Streamlinr;

/// <summary>
/// Provides message metadata to serializers.
/// </summary>
public sealed class SerializationContext {
    /// <summary>
    /// Initializes a new serialization context.
    /// </summary>
    /// <param name="topic">The Kafka topic.</param>
    /// <param name="headers">The message headers.</param>
    public SerializationContext(String topic, MessageHeaders headers) {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentNullException.ThrowIfNull(headers);

        Topic = topic;
        Headers = headers;
    }

    /// <summary>
    /// Gets the Kafka topic.
    /// </summary>
    public String Topic { get; }

    /// <summary>
    /// Gets the message headers.
    /// </summary>
    public MessageHeaders Headers { get; }
}
