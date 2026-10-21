namespace Streamlinr;

using static ValueFailureAction;

/// <summary>
/// Defines how a stream source handles failures while resolving or deserializing message values.
/// </summary>
/// <param name="Unresolved">The behavior for values whose message type could not be resolved.</param>
/// <param name="DeserializationFailed">The behavior for values whose message type resolved but failed to deserialize.</param>
public sealed record ValueFailure(ValueFailureAction Unresolved, ValueFailureAction DeserializationFailed) {
    /// <summary>
    /// Continue processing all value failures by representing the failed value as <see cref="StreamValue.DeadLetter" />.
    /// </summary>
    static public ValueFailure ContinueAsDeadLetter() => On(
        unresolved           : new ValueFailureAction.ContinueAsDeadLetter(),
        deserializationFailed: new ValueFailureAction.ContinueAsDeadLetter());

    /// <summary>
    /// Defines separate behavior for type resolution and deserialization failures.
    /// </summary>
    static public ValueFailure On(ValueFailureAction unresolved, ValueFailureAction deserializationFailed) {
        return new ValueFailure(unresolved, deserializationFailed);
    }
}

/// <summary>
/// Defines a terminal action for a value resolution or deserialization failure.
/// </summary>
public readonly union ValueFailureAction(Skip, ContinueAsDeadLetter, PausePartition) {
    /// <summary>
    /// Skip the failed record and continue processing later records.
    /// </summary>
    public record struct Skip;

    /// <summary>
    /// Continue processing by representing the failed value as <see cref="StreamValue.DeadLetter" />.
    /// </summary>
    public record struct ContinueAsDeadLetter;

    /// <summary>
    /// Pause processing for the failed record's partition until the runtime is stopped.
    /// </summary>
    public record struct PausePartition;
}
