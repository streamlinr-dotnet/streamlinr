namespace Streamlinr;

/// <summary>
/// Builds a Streamlinr topology declaration.
/// </summary>
public class TopologyBuilder {
    readonly List<SourceDeclaration> _sources = [];
    readonly List<MergeDeclaration> _merges = [];
    readonly List<ProcessorDeclaration> _processors = [];

    internal IReadOnlyList<SourceDeclaration> Sources => _sources;

    internal IReadOnlyList<MergeDeclaration> Merges => _merges;

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

        return new StreamBuilder<TKey, TValue>(this, source.SourceId, [source.SourceId]);
    }

    /// <summary>
    /// Merges two streams with the same key and value types into one downstream stream.
    /// </summary>
    /// <typeparam name="TKey">The record key type.</typeparam>
    /// <typeparam name="TValue">The record value type.</typeparam>
    /// <param name="first">The first stream to merge.</param>
    /// <param name="second">The second stream to merge.</param>
    /// <returns>A builder for processors that observe records from both streams.</returns>
    public StreamBuilder<TKey, TValue> Merge<TKey, TValue>(StreamBuilder<TKey, TValue> first, StreamBuilder<TKey, TValue> second) {
        ArgumentNullException.ThrowIfNull(first);
        ArgumentNullException.ThrowIfNull(second);

        if (!ReferenceEquals(first.Topology, this) || !ReferenceEquals(second.Topology, this))
            throw new InvalidOperationException("Streams must belong to the same topology.");

        var sourceIds = first.SourceIds.Concat(second.SourceIds).Distinct().ToArray();
        var merge = new MergeDeclaration(
            MergeId  : $"merge-{_merges.Count + 1}",
            SourceIds: sourceIds
        );

        _merges.Add(merge);

        return new StreamBuilder<TKey, TValue>(this, merge.MergeId, sourceIds);
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
    readonly String _streamId;
    readonly IReadOnlyList<String> _sourceIds;

    internal StreamBuilder(TopologyBuilder topology, String streamId, IReadOnlyList<String> sourceIds) {
        _topology = topology;
        _streamId = streamId;
        _sourceIds = sourceIds;
    }

    internal TopologyBuilder Topology => _topology;

    internal IReadOnlyList<String> SourceIds => _sourceIds;

    /// <summary>
    /// Executes a side-effect callback for each record observed on the stream.
    /// </summary>
    /// <param name="callback">The callback to execute for each record.</param>
    /// <returns>The current stream builder.</returns>
    public StreamBuilder<TKey, TValue> Peek(Func<StreamRecord<TKey, TValue>, CancellationToken, ValueTask> callback) {
        ArgumentNullException.ThrowIfNull(callback);

        _topology.AddProcessor(new ProcessorDeclaration(
            ProcessorId: $"peek-{_topology.Processors.Count + 1}",
            StreamId   : _streamId,
            SourceIds  : _sourceIds,
            Callback   : (key, value, cancellationToken) => callback(new StreamRecord<TKey, TValue>((TKey)(Object)key, (TValue)(Object)value), cancellationToken).AsTask()
        ));

        return this;
    }
}

sealed record SourceDeclaration(String SourceId, String Topic, Type KeyType, Type ValueType);
sealed record MergeDeclaration(String MergeId, IReadOnlyList<String> SourceIds);
sealed record ProcessorDeclaration(String ProcessorId, String StreamId, IReadOnlyList<String> SourceIds, Func<String, String, CancellationToken, Task> Callback);
