namespace Streamlinr;

using static DeadLetterHandling;

/// <summary>
/// Defines how a topic sink handles <see cref="StreamValue.DeadLetter" /> values.
/// </summary>
public readonly union DeadLetterHandling(Fail, Skip) {
    /// <summary>
    /// Treat a dead-letter value reaching the sink as a sink failure.
    /// </summary>
    public record struct Fail;

    /// <summary>
    /// Skip dead-letter values and continue processing later records.
    /// </summary>
    public record struct Skip;
}
