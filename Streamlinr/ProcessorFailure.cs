namespace Streamlinr;

/// <summary>
/// Defines how a processor handles unhandled exceptions from user callback code.
/// </summary>
public abstract record ProcessorFailure {
    private protected ProcessorFailure() {
    }

    /// <summary>
    /// Treat an unhandled processor exception as fatal to the topology.
    /// </summary>
    static public ProcessorFailure FailTopology() => new FailTopologyPolicy();

    /// <summary>
    /// Skip the failed record and continue processing later records.
    /// </summary>
    static public ProcessorFailure Skip() => new SkipPolicy();

    /// <summary>
    /// Continue downstream processing by representing the failed record as <see cref="StreamValue.DeadLetter" />.
    /// </summary>
    static public ProcessorFailure ContinueAsDeadLetter() => new ContinueAsDeadLetterPolicy();

    /// <summary>
    /// Pause processing for the failed record's partition until the runtime is stopped.
    /// </summary>
    static public ProcessorFailure PausePartition() => new PausePartitionPolicy();

    internal sealed record FailTopologyPolicy : ProcessorFailure;

    internal sealed record SkipPolicy : ProcessorFailure;

    internal sealed record ContinueAsDeadLetterPolicy : ProcessorFailure;

    internal sealed record PausePartitionPolicy : ProcessorFailure;
}
