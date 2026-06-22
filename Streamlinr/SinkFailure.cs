namespace Streamlinr;

/// <summary>
/// Defines how a terminal sink handles failures while writing output.
/// </summary>
public abstract record SinkFailure {
    private protected SinkFailure() {
    }

    /// <summary>
    /// Treat a sink failure as fatal to the topology.
    /// </summary>
    static public SinkFailure FailTopology() => new FailTopologyPolicy();

    /// <summary>
    /// Skip the failed sink write and continue processing later records.
    /// </summary>
    static public SinkFailure Skip() => new SkipPolicy();

    /// <summary>
    /// Pause processing for the failed record's partition until the runtime is stopped.
    /// </summary>
    static public SinkFailure PausePartition() => new PausePartitionPolicy();

    internal sealed record FailTopologyPolicy : SinkFailure;

    internal sealed record SkipPolicy : SinkFailure;

    internal sealed record PausePartitionPolicy : SinkFailure;
}
