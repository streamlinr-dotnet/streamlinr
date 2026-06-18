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

    internal sealed record FailTopologyPolicy : ProcessorFailure;
}
