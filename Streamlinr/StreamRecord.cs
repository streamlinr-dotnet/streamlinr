namespace Streamlinr;

/// <summary>
/// A record observed by a Streamlinr processor.
/// </summary>
/// <typeparam name="TKey">The record key type.</typeparam>
public sealed record StreamRecord<TKey>(TKey Key, StreamValue Value);

/// <summary>
/// Represents the state of a Kafka message value observed by Streamlinr.
/// </summary>
public abstract record StreamValue {
    /// <summary>
    /// The message value was resolved to a CLR type and deserialized successfully.
    /// </summary>
    /// <param name="Value">The deserialized value.</param>
    /// <param name="Type">The resolved CLR value type.</param>
    public sealed record Resolved(Object Value, Type Type) : StreamValue;

    /// <summary>
    /// The Kafka message value was null.
    /// </summary>
    public sealed record Tombstone : StreamValue;

    /// <summary>
    /// The message value bytes were present, but no CLR type could be resolved.
    /// </summary>
    /// <param name="Data">The raw message value bytes.</param>
    /// <param name="Reason">The reason type resolution failed.</param>
    /// <param name="Headers">The message headers.</param>
    public sealed record Unresolved(Byte[] Data, String Reason, MessageHeaders Headers) : StreamValue;

    /// <summary>
    /// The message value type was resolved, but deserialization failed.
    /// </summary>
    /// <param name="Data">The raw message value bytes.</param>
    /// <param name="Type">The resolved CLR value type.</param>
    /// <param name="Error">The deserialization error.</param>
    /// <param name="Headers">The message headers.</param>
    public sealed record DeserializationFailed(Byte[] Data, Type Type, Exception Error, MessageHeaders Headers) : StreamValue;
}
