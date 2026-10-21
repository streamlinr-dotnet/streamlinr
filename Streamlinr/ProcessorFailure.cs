namespace Streamlinr;

using static ProcessorFailure;

/// <summary>
/// Defines how a processor handles unhandled exceptions from user callback code.
/// </summary>
public readonly union ProcessorFailure(FailTopology, Skip, ContinueAsDeadLetter, PausePartition) {
    /// <summary>
    /// Treat an unhandled processor exception as fatal to the topology.
    /// </summary>
    public record struct FailTopology;

    /// <summary>
    /// Skip the failed record and continue processing later records.
    /// </summary>
    public record struct Skip;

    /// <summary>
    /// Continue downstream processing by representing the failed record as <see cref="StreamValue.DeadLetter" />.
    /// </summary>
    public record struct ContinueAsDeadLetter;

    /// <summary>
    /// Pause processing for the failed record's partition until the runtime is stopped.
    /// </summary>
    public record struct PausePartition;
}
