namespace Streamlinr;

/// <summary>
/// Defines how a stream source handles failures while resolving or deserializing message values.
/// </summary>
public abstract record ValueFailure {
    private protected ValueFailure() {
    }

    /// <summary>
    /// Continue processing by representing the failed value as <see cref="StreamValue.DeadLetter" />.
    /// </summary>
    static public ValueFailure ContinueAsDeadLetter() => new ContinueAsDeadLetterPolicy();

    internal sealed record ContinueAsDeadLetterPolicy : ValueFailure;
}
