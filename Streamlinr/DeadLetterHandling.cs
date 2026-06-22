namespace Streamlinr;

/// <summary>
/// Defines how a topic sink handles <see cref="StreamValue.DeadLetter" /> values.
/// </summary>
public abstract record DeadLetterHandling {
    private protected DeadLetterHandling() {
    }

    /// <summary>
    /// Treat a dead-letter value reaching the sink as a sink failure.
    /// </summary>
    static public DeadLetterHandling Fail() => new FailPolicy();

    /// <summary>
    /// Skip dead-letter values and continue processing later records.
    /// </summary>
    static public DeadLetterHandling Skip() => new SkipPolicy();

    internal sealed record FailPolicy : DeadLetterHandling;

    internal sealed record SkipPolicy : DeadLetterHandling;
}
