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
    /// Value processing failed at a boundary configured to continue as dead-letter data.
    /// </summary>
    /// <param name="KeyData">The original key data when available.</param>
    /// <param name="ValueData">The failed value data.</param>
    /// <param name="Reason">The reason the value was dead-lettered.</param>
    /// <param name="Error">The failure error when available.</param>
    /// <param name="Headers">The message headers.</param>
    public sealed record DeadLetter(Object? KeyData, Object? ValueData, String Reason, Exception? Error, MessageHeaders Headers) : StreamValue;
}
