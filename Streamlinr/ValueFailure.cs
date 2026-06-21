namespace Streamlinr;

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
        unresolved           : ValueFailureAction.ContinueAsDeadLetter(),
        deserializationFailed: ValueFailureAction.ContinueAsDeadLetter());

    /// <summary>
    /// Defines separate behavior for type resolution and deserialization failures.
    /// </summary>
    static public ValueFailure On(ValueFailureAction unresolved, ValueFailureAction deserializationFailed) {
        ArgumentNullException.ThrowIfNull(unresolved);
        ArgumentNullException.ThrowIfNull(deserializationFailed);

        return new ValueFailure(unresolved, deserializationFailed);
    }
}

/// <summary>
/// Defines a terminal action for a value resolution or deserialization failure.
/// </summary>
public abstract record ValueFailureAction {
    private protected ValueFailureAction() {
    }

    /// <summary>
    /// Skip the failed record and continue processing later records.
    /// </summary>
    static public ValueFailureAction Skip() => new SkipPolicy();

    /// <summary>
    /// Continue processing by representing the failed value as <see cref="StreamValue.DeadLetter" />.
    /// </summary>
    static public ValueFailureAction ContinueAsDeadLetter() => new ContinueAsDeadLetterPolicy();

    /// <summary>
    /// Pause processing for the failed record's partition until the runtime is stopped.
    /// </summary>
    static public ValueFailureAction PausePartition() => new PausePartitionPolicy();

    internal sealed record SkipPolicy : ValueFailureAction;

    internal sealed record ContinueAsDeadLetterPolicy : ValueFailureAction;

    internal sealed record PausePartitionPolicy : ValueFailureAction;
}
