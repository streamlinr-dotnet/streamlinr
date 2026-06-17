namespace Streamlinr;

using System.Reflection;

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

        var sourcePlans = topology.Sources
            .Select(source => new SourcePlan(
                source.SourceId,
                source.Topic,
                source.KeyType.FullName ?? source.KeyType.Name,
                CreateKeyDeserializer(source.KeyType, source.KeySerializer),
                CreateValueDeserializer(source.ValueSerializer, source.MessageTypeResolver)
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

    static RuntimeDeserializer CreateKeyDeserializer(Type valueType, Object serializer) {
        var method = typeof(StreamlinrApplication)
            .GetMethod(nameof(CreateKeyDeserializerCore), BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(valueType);

        return (RuntimeDeserializer)method.Invoke(null, [serializer])!;
    }

    static RuntimeDeserializer CreateKeyDeserializerCore<T>(IKeySerializer<T> serializer) =>
        new RuntimeDeserializer((data, context) => {
            var headers              = new MessageHeaders(context.Headers.Select(header => (header.Name, header.Value)));
            var serializationContext = new SerializationContext(context.Topic, headers);

            return serializer.Deserialize(data, serializationContext)!;
        });

    static RuntimeDeserializer CreateValueDeserializer(IValueSerializer serializer, IMessageTypeResolver messageTypeResolver) =>
        new RuntimeDeserializer((data, context) => {
            var headers              = new MessageHeaders(context.Headers.Select(header => (header.Name, header.Value)));
            var serializationContext = new SerializationContext(context.Topic, headers);

            if (data is null) return new StreamValue.Tombstone();

            return messageTypeResolver.ResolveType(serializationContext) switch {
                MessageTypeResolution.Resolved resolved => DeserializeValue(serializer, data, resolved.Type, serializationContext, headers),
                MessageTypeResolution.Unresolved unresolved => new StreamValue.Unresolved(data, unresolved.Reason, headers),
                _ => new StreamValue.Unresolved(data, "Message type resolver returned an unsupported resolution result.", headers),
            };
        });

    static StreamValue DeserializeValue(IValueSerializer serializer, Byte[] data, Type valueType, SerializationContext context, MessageHeaders headers) {
        try {
            return new StreamValue.Resolved(serializer.Deserialize(data, valueType, context), valueType);
        }
        catch (Exception error) {
            return new StreamValue.DeserializationFailed(data, valueType, error, headers);
        }
    }
}
