namespace Streamlinr;

/// <summary>
/// Resolves CLR message value types from message metadata and writes type metadata for outgoing messages.
/// </summary>
public interface IMessageTypeResolver {
    /// <summary>
    /// Resolves the CLR type for an incoming Kafka message value.
    /// </summary>
    /// <param name="context">The serialization context.</param>
    /// <returns>The message type resolution result.</returns>
    MessageTypeResolution ResolveType(SerializationContext context);

    /// <summary>
    /// Writes type metadata for an outgoing Kafka message value.
    /// </summary>
    /// <param name="valueType">The CLR value type.</param>
    /// <param name="context">The serialization context.</param>
    void WriteType(Type valueType, SerializationContext context);
}

/// <summary>
/// The result of resolving a Kafka message value to a CLR type.
/// </summary>
public abstract record MessageTypeResolution {
    /// <summary>
    /// Type resolution succeeded.
    /// </summary>
    /// <param name="Type">The resolved CLR type.</param>
    public sealed record Resolved(Type Type) : MessageTypeResolution;

    /// <summary>
    /// Type resolution did not succeed, but the message should continue flowing through the stream.
    /// </summary>
    /// <param name="Reason">The reason the type could not be resolved.</param>
    public sealed record Unresolved(String Reason) : MessageTypeResolution;
}
