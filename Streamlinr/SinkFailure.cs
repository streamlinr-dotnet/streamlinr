namespace Streamlinr;

using static SinkFailure;

/// <summary>
/// Defines how a terminal sink handles failures while writing output.
/// </summary>
public readonly union SinkFailure(FailTopology, Skip, PausePartition) {
    /// <summary>
    /// Treat a sink failure as fatal to the topology.
    /// </summary>
    public record struct FailTopology;

    /// <summary>
    /// Skip the failed sink write and continue processing later records.
    /// </summary>
    public record struct Skip;

    /// <summary>
    /// Pause processing for the failed record's partition until the runtime is stopped.
    /// </summary>
    public record struct PausePartition;
}
