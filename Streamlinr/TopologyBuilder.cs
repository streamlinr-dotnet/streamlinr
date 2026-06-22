namespace Streamlinr;

/// <summary>
/// Builds a Streamlinr topology declaration.
/// </summary>
public class TopologyBuilder {
    readonly List<SourceDeclaration> _sources = [];
    readonly List<MergeDeclaration> _merges = [];
    readonly List<ProcessorDeclaration> _processors = [];
    readonly List<SinkDeclaration> _sinks = [];

    internal IReadOnlyList<SourceDeclaration> Sources => _sources;

    internal IReadOnlyList<MergeDeclaration> Merges => _merges;

    internal IReadOnlyList<ProcessorDeclaration> Processors => _processors;

    internal IReadOnlyList<SinkDeclaration> Sinks => _sinks;

    /// <summary>
    /// Declares a stream sourced from a Kafka topic using an implicit built-in key serializer.
    /// </summary>
    /// <typeparam name="TKey">The record key type.</typeparam>
    /// <param name="topic">The source topic name.</param>
    /// <param name="valueSerializer">The message value serializer.</param>
    /// <param name="messageTypeResolver">The message type resolver.</param>
    /// <param name="failure">The value failure behavior.</param>
    /// <returns>A builder for stream processors.</returns>
    /// <remarks>
    /// Values are polymorphic: <paramref name="messageTypeResolver" /> resolves each message value to a CLR type
    /// before <paramref name="valueSerializer" /> deserializes it. Unknown message types, tombstones, and
    /// deserialization failures continue through the stream as <see cref="StreamValue" /> cases.
    /// Built-in key serializers are available for <see cref="String" />, <see cref="Byte" /> arrays,
    /// <see cref="Int32" />, <see cref="Int64" />, and <see cref="Guid" />. Use the overload that accepts an
    /// explicit key serializer for any other key type.
    /// </remarks>
    public StreamBuilder<TKey> Stream<TKey>(String topic, IValueSerializer valueSerializer, IMessageTypeResolver messageTypeResolver, ValueFailure failure) {
        var keySerializer = ResolveDefaultKeySerializer<TKey>();

        if (keySerializer is null)
            throw new InvalidOperationException("No default key serializer is available for the requested stream key type. Provide a key serializer explicitly.");

        return Stream(topic, keySerializer, valueSerializer, messageTypeResolver, failure);
    }

    /// <summary>
    /// Declares a stream sourced from a Kafka topic.
    /// </summary>
    /// <typeparam name="TKey">The record key type.</typeparam>
    /// <param name="topic">The source topic name.</param>
    /// <param name="keySerializer">The message key serializer.</param>
    /// <param name="valueSerializer">The message value serializer.</param>
    /// <param name="messageTypeResolver">The message type resolver.</param>
    /// <param name="failure">The value failure behavior.</param>
    /// <returns>A builder for stream processors.</returns>
    public StreamBuilder<TKey> Stream<TKey>(String topic, IKeySerializer<TKey> keySerializer, IValueSerializer valueSerializer, IMessageTypeResolver messageTypeResolver, ValueFailure failure) {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentNullException.ThrowIfNull(keySerializer);
        ArgumentNullException.ThrowIfNull(valueSerializer);
        ArgumentNullException.ThrowIfNull(messageTypeResolver);
        ArgumentNullException.ThrowIfNull(failure);

        var source = new SourceDeclaration(
            SourceId           : $"source-{_sources.Count + 1}",
            Topic              : topic,
            KeyType            : typeof(TKey),
            KeySerializer      : keySerializer,
            ValueSerializer    : valueSerializer,
            MessageTypeResolver: messageTypeResolver,
            Failure            : failure
        );

        _sources.Add(source);

        return new StreamBuilder<TKey>(this, source.SourceId, [source.SourceId]);
    }

    /// <summary>
    /// Merges two streams with the same key type into one downstream stream.
    /// </summary>
    /// <typeparam name="TKey">The record key type.</typeparam>
    /// <param name="first">The first stream to merge.</param>
    /// <param name="second">The second stream to merge.</param>
    /// <returns>A builder for processors that observe records from both streams.</returns>
    public StreamBuilder<TKey> Merge<TKey>(StreamBuilder<TKey> first, StreamBuilder<TKey> second) {
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

        return new StreamBuilder<TKey>(this, merge.MergeId, sourceIds);
    }

    internal void AddProcessor(ProcessorDeclaration processor) => _processors.Add(processor);

    internal void AddSink(SinkDeclaration sink) => _sinks.Add(sink);

    internal static IKeySerializer<T>? ResolveDefaultKeySerializer<T>() {
        if (typeof(T) == typeof(String))
            return (IKeySerializer<T>)(Object)KeySerializers.String;

        if (typeof(T) == typeof(Byte[]))
            return (IKeySerializer<T>)(Object)KeySerializers.Bytes;

        if (typeof(T) == typeof(Int32))
            return (IKeySerializer<T>)(Object)KeySerializers.Int32;

        if (typeof(T) == typeof(Int64))
            return (IKeySerializer<T>)(Object)KeySerializers.Int64;

        if (typeof(T) == typeof(Guid))
            return (IKeySerializer<T>)(Object)KeySerializers.Guid;

        return null;
    }
}

/// <summary>
/// Builds processors for a declared stream.
/// </summary>
/// <typeparam name="TKey">The record key type.</typeparam>
public sealed class StreamBuilder<TKey> {
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
    /// <param name="failure">The processor failure behavior.</param>
    /// <returns>The current stream builder.</returns>
    public StreamBuilder<TKey> Peek(Func<StreamRecord<TKey>, CancellationToken, ValueTask> callback, ProcessorFailure failure) {
        ArgumentNullException.ThrowIfNull(callback);
        ArgumentNullException.ThrowIfNull(failure);

        _topology.AddProcessor(new ProcessorDeclaration(
            ProcessorId: $"peek-{_topology.Processors.Count + 1}",
            StreamId   : _streamId,
            SourceIds  : _sourceIds,
            Callback   : (key, value, cancellationToken) => callback(new StreamRecord<TKey>((TKey)key, (StreamValue)value), cancellationToken).AsTask(),
            Failure    : failure
        ));

        return this;
    }

    /// <summary>
    /// Writes resolved stream values and tombstones to a Kafka topic using an implicit built-in key serializer.
    /// </summary>
    public void ToTopic(String topic, IValueSerializer valueSerializer, IMessageTypeResolver messageTypeResolver, DeadLetterHandling deadLetters, ProcessorFailure failure) {
        var keySerializer = TopologyBuilder.ResolveDefaultKeySerializer<TKey>();

        if (keySerializer is null)
            throw new InvalidOperationException("No default key serializer is available for the requested stream key type. Provide a key serializer explicitly.");

        ToTopic(topic, keySerializer, valueSerializer, messageTypeResolver, deadLetters, failure);
    }

    /// <summary>
    /// Writes resolved stream values and tombstones to a Kafka topic.
    /// </summary>
    public void ToTopic(String topic, IKeySerializer<TKey> keySerializer, IValueSerializer valueSerializer, IMessageTypeResolver messageTypeResolver, DeadLetterHandling deadLetters, ProcessorFailure failure) {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentNullException.ThrowIfNull(keySerializer);
        ArgumentNullException.ThrowIfNull(valueSerializer);
        ArgumentNullException.ThrowIfNull(messageTypeResolver);
        ArgumentNullException.ThrowIfNull(deadLetters);
        ArgumentNullException.ThrowIfNull(failure);

        _topology.AddSink(new SinkDeclaration(
            SinkId             : $"sink-{_topology.Sinks.Count + 1}",
            StreamId           : _streamId,
            SourceIds          : _sourceIds,
            Topic              : topic,
            KeyType            : typeof(TKey),
            KeySerializer      : keySerializer,
            ValueSerializer    : valueSerializer,
            MessageTypeResolver: messageTypeResolver,
            DeadLetters        : deadLetters,
            Failure            : failure
        ));
    }
}

sealed record SourceDeclaration(String SourceId, String Topic, Type KeyType, Object KeySerializer, IValueSerializer ValueSerializer, IMessageTypeResolver MessageTypeResolver, ValueFailure Failure);
sealed record MergeDeclaration(String MergeId, IReadOnlyList<String> SourceIds);
sealed record ProcessorDeclaration(String ProcessorId, String StreamId, IReadOnlyList<String> SourceIds, Func<Object, Object, CancellationToken, Task> Callback, ProcessorFailure Failure);
sealed record SinkDeclaration(String SinkId, String StreamId, IReadOnlyList<String> SourceIds, String Topic, Type KeyType, Object KeySerializer, IValueSerializer ValueSerializer, IMessageTypeResolver MessageTypeResolver, DeadLetterHandling DeadLetters, ProcessorFailure Failure);
