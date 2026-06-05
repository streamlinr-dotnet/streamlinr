namespace Streamlinr;

/// <summary>
/// Builds a Streamlinr topology declaration.
/// </summary>
public class TopologyBuilder {
    readonly List<SourceDeclaration> _sources = [];
    readonly List<ProcessorDeclaration> _processors = [];

    internal IReadOnlyList<SourceDeclaration> Sources => _sources;

    internal IReadOnlyList<ProcessorDeclaration> Processors => _processors;

    /// <summary>
    /// Declares a stream sourced from a Kafka topic.
    /// </summary>
    /// <typeparam name="TKey">The record key type.</typeparam>
    /// <typeparam name="TValue">The record value type.</typeparam>
    /// <param name="topic">The source topic name.</param>
    /// <returns>A builder for stream processors.</returns>
    public StreamBuilder<TKey, TValue> Stream<TKey, TValue>(String topic) {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);

        var source = new SourceDeclaration(
            SourceId : $"source-{_sources.Count + 1}",
            Topic    : topic,
            KeyType  : typeof(TKey),
            ValueType: typeof(TValue)
        );

        _sources.Add(source);

        return new StreamBuilder<TKey, TValue>(this, source);
    }

    internal void AddProcessor(ProcessorDeclaration processor) => _processors.Add(processor);
}

/// <summary>
/// Builds processors for a declared stream.
/// </summary>
/// <typeparam name="TKey">The record key type.</typeparam>
/// <typeparam name="TValue">The record value type.</typeparam>
public sealed class StreamBuilder<TKey, TValue> {
    readonly TopologyBuilder _topology;
    readonly SourceDeclaration _source;

    internal StreamBuilder(TopologyBuilder topology, SourceDeclaration source) {
        _topology = topology;
        _source = source;
    }

    /// <summary>
    /// Executes a side-effect callback for each record observed on the stream.
    /// </summary>
    /// <param name="callback">The callback to execute for each record.</param>
    /// <returns>The current stream builder.</returns>
    public StreamBuilder<TKey, TValue> Peek(Func<StreamRecord<TKey, TValue>, CancellationToken, ValueTask> callback) {
        ArgumentNullException.ThrowIfNull(callback);

        _topology.AddProcessor(new ProcessorDeclaration(
            ProcessorId: $"peek-{_topology.Processors.Count + 1}",
            SourceId   : _source.SourceId,
            Callback   : (key, value, cancellationToken) => callback(new StreamRecord<TKey, TValue>((TKey)(Object)key, (TValue)(Object)value), cancellationToken).AsTask()
        ));

        return this;
    }
}

sealed record SourceDeclaration(String SourceId, String Topic, Type KeyType, Type ValueType);
sealed record ProcessorDeclaration(String ProcessorId, String SourceId, Func<String, String, CancellationToken, Task> Callback);
