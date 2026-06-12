namespace Streamlinr;

/// <summary>
/// Starts Streamlinr topologies using the initial explicit lifecycle API.
/// </summary>
static public class StreamlinrApplication {
    /// <summary>
    /// Runs a Streamlinr topology until cancellation or fatal runtime failure.
    /// </summary>
    /// <param name="options">The runtime options.</param>
    /// <param name="configureTopology">The topology declaration callback.</param>
    /// <param name="cancellationToken">A cancellation token used to stop the runtime.</param>
    /// <returns>A task that completes when the runtime stops.</returns>
    static public Task RunAsync(StreamlinrOptions options, Action<TopologyBuilder> configureTopology, CancellationToken cancellationToken = default) {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(configureTopology);

        var topology = new TopologyBuilder();
        configureTopology(topology);

        var plan = Compile(options, topology);
        return Runtime.RunAsync(plan, cancellationToken);
    }

    static RuntimePlan Compile(StreamlinrOptions options, TopologyBuilder topology) {
        if (String.IsNullOrWhiteSpace(options.ApplicationId))
            throw new InvalidOperationException("ApplicationId is required.");

        if (String.IsNullOrWhiteSpace(options.BootstrapServers))
            throw new InvalidOperationException("BootstrapServers is required.");

        if (topology.Sources.Count == 0)
            throw new InvalidOperationException("At least one stream source is required.");

        if (topology.Processors.Count == 0)
            throw new InvalidOperationException("At least one processor is required.");

        if (topology.Sources.Any(source => source.KeyType != typeof(String) || source.ValueType != typeof(String)))
            throw new InvalidOperationException("The initial runtime supports only string keys and string values.");

        var sourcePlans = topology.Sources
            .Select(source => new SourcePlan(
                source.SourceId,
                source.Topic,
                source.KeyType.FullName ?? source.KeyType.Name,
                source.ValueType.FullName ?? source.ValueType.Name
            ))
            .ToArray();

        var processorPlans = topology.Processors
            .Select(processor => new ProcessorPlan(
                processor.ProcessorId,
                processor.SourceIds.ToArray(),
                (record, token) => processor.Callback(record.Key, record.Value, token))
            )
            .ToArray();

        var topologyPlan = new TopologyPlan("default", sourcePlans, processorPlans);

        return new RuntimePlan(options.ApplicationId, options.BootstrapServers, [topologyPlan]);
    }
}
