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

        if (topology.Processors.Count == 0 && topology.Sinks.Count == 0)
            throw new InvalidOperationException("At least one processor or sink is required.");

        var sourcePlans = topology.Sources
            .Select(source => new SourcePlan(
                source.SourceId,
                source.Topic,
                source.KeyType.FullName ?? source.KeyType.Name,
                CreateKeyDeserializer(source.KeyType, source.KeySerializer),
                CreateValueDeserializer(source.ValueSerializer, source.MessageTypeResolver, source.Failure)
            ))
            .ToArray();

        var processorPlans = topology.Processors
            .Select(processor => {
                ValidateProcessorFailure(processor.Failure);

                return new ProcessorPlan(
                    processor.ProcessorId,
                    processor.SourceIds.ToArray(),
                    (record, token) => processor.Callback(record.Key, record.Value, token),
                    ToRuntimeProcessorFailureAction(processor.Failure),
                    CreateProcessorDeadLetter);
            })
            .ToArray();

        var sinkPlans = topology.Sinks
            .Select(sink => {
                return new SinkPlan(
                    sink.SinkId,
                    sink.SourceIds.ToArray(),
                    CreateSinkEncoder(sink),
                    ToRuntimeSinkFailureAction(sink.Failure));
            })
            .ToArray();

        var topologyPlan = new TopologyPlan("default", sourcePlans, processorPlans, sinkPlans);

        return new RuntimePlan(options.ApplicationId, options.BootstrapServers, [topologyPlan]);
    }

    static RuntimeDeserializer CreateKeyDeserializer(Type valueType, Object serializer) {
        var method = typeof(StreamlinrApplication)
            .GetMethod(nameof(CreateKeyDeserializerCore), BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(valueType);

        return (RuntimeDeserializer)method.Invoke(null, [serializer])!;
    }

    static RuntimeSinkEncoder CreateSinkEncoder(SinkDeclaration sink) {
        var method = typeof(StreamlinrApplication)
            .GetMethod(nameof(CreateSinkEncoderCore), BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(sink.KeyType);

        return (RuntimeSinkEncoder)method.Invoke(null, [sink])!;
    }

    static RuntimeDeserializer CreateKeyDeserializerCore<T>(IKeySerializer<T> serializer) =>
        new RuntimeDeserializer((data, context) => {
            var headers              = new MessageHeaders(context.Headers.Select(header => (header.Name, header.Value)));
            var serializationContext = new SerializationContext(context.Topic, headers);

            return serializer.Deserialize(data, serializationContext)!;
        });

    static RuntimeSinkEncoder CreateSinkEncoderCore<TKey>(SinkDeclaration sink) =>
        new RuntimeSinkEncoder(record => {
            var keySerializer = (IKeySerializer<TKey>)sink.KeySerializer;

            return record.Value switch {
                StreamValue.Resolved resolved => CreateResolvedSinkRecord(sink, keySerializer, (TKey)record.Key, resolved),
                StreamValue.Tombstone => CreateTombstoneSinkRecord(sink, keySerializer, (TKey)record.Key),
                StreamValue.DeadLetter deadLetter => ApplyDeadLetterHandling(sink.DeadLetters, deadLetter),
                _ => throw new InvalidOperationException($"Unsupported stream value type '{record.Value.GetType().FullName}'."),
            };
        });

    static RuntimeSinkResult CreateResolvedSinkRecord<TKey>(SinkDeclaration sink, IKeySerializer<TKey> keySerializer, TKey key, StreamValue.Resolved value) {
        var headers = new MessageHeaders();
        var context = new SerializationContext(sink.Topic, headers);
        sink.MessageTypeResolver.WriteType(value.Type, context);

        return RuntimeSinkResult.Produce(new RuntimeSinkRecord(
            sink.Topic,
            keySerializer.Serialize(key, context),
            sink.ValueSerializer.Serialize(value.Value, value.Type, context),
            headers.All.Select(header => new RuntimeHeader(header.Name, header.Value)).ToArray()));
    }

    static RuntimeSinkResult CreateTombstoneSinkRecord<TKey>(SinkDeclaration sink, IKeySerializer<TKey> keySerializer, TKey key) {
        var headers = new MessageHeaders();
        var context = new SerializationContext(sink.Topic, headers);

        return RuntimeSinkResult.Produce(new RuntimeSinkRecord(
            sink.Topic,
            keySerializer.Serialize(key, context),
            null!,
            headers.All.Select(header => new RuntimeHeader(header.Name, header.Value)).ToArray()));
    }

    static RuntimeSinkResult ApplyDeadLetterHandling(DeadLetterHandling deadLetters, StreamValue.DeadLetter deadLetter) => deadLetters switch {
        DeadLetterHandling.FailPolicy => throw new InvalidOperationException($"Dead-letter value reached topic sink: {deadLetter.Reason}"),
        DeadLetterHandling.SkipPolicy => RuntimeSinkResult.Skip(),
        _ => throw new InvalidOperationException("The requested dead-letter handling policy is not supported by this runtime."),
    };

    static void ValidateProcessorFailure(ProcessorFailure failure) {
        if (failure is not (ProcessorFailure.FailTopologyPolicy or ProcessorFailure.SkipPolicy or ProcessorFailure.ContinueAsDeadLetterPolicy or ProcessorFailure.PausePartitionPolicy))
            throw new InvalidOperationException("The requested processor failure policy is not supported by this runtime.");
    }

    static RuntimeProcessorFailureAction ToRuntimeProcessorFailureAction(ProcessorFailure failure) => failure switch {
        ProcessorFailure.FailTopologyPolicy => RuntimeProcessorFailureAction.FailTopology,
        ProcessorFailure.SkipPolicy => RuntimeProcessorFailureAction.Skip,
        ProcessorFailure.ContinueAsDeadLetterPolicy => RuntimeProcessorFailureAction.ContinueAsDeadLetter,
        ProcessorFailure.PausePartitionPolicy => RuntimeProcessorFailureAction.PausePartition,
        _ => throw new InvalidOperationException("The requested processor failure policy is not supported by this runtime."),
    };

    static RuntimeProcessorFailureAction ToRuntimeSinkFailureAction(SinkFailure failure) => failure switch {
        SinkFailure.FailTopologyPolicy => RuntimeProcessorFailureAction.FailTopology,
        SinkFailure.SkipPolicy => RuntimeProcessorFailureAction.Skip,
        SinkFailure.PausePartitionPolicy => RuntimeProcessorFailureAction.PausePartition,
        _ => throw new InvalidOperationException("The requested sink failure policy is not supported by this runtime."),
    };

    static Object CreateProcessorDeadLetter(SourceRecord record, Exception error) {
        var headers = new MessageHeaders(record.Headers.Select(header => (header.Name, header.Value)));
        return new StreamValue.DeadLetter(record.Key, record.Value, $"Processor failed at topic '{record.Topic}', partition {record.Partition}, offset {record.Offset}.", error, headers);
    }

    static RuntimeValueDeserializer CreateValueDeserializer(IValueSerializer serializer, IMessageTypeResolver messageTypeResolver, ValueFailure failure) {
        return new RuntimeValueDeserializer((data, context) => {
            var headers              = new MessageHeaders(context.Headers.Select(header => (header.Name, header.Value)));
            var serializationContext = new SerializationContext(context.Topic, headers);

            if (data is null) return RuntimeValueResult.Emit(new StreamValue.Tombstone());

            return messageTypeResolver.ResolveType(serializationContext) switch {
                MessageTypeResolution.Resolved resolved => DeserializeValue(serializer, data, resolved.Type, serializationContext, headers, failure.DeserializationFailed),
                MessageTypeResolution.Unresolved unresolved => ApplyValueFailure(failure.Unresolved, new StreamValue.DeadLetter(null, data, unresolved.Reason, null, headers)),
                _ => ApplyValueFailure(failure.Unresolved, new StreamValue.DeadLetter(null, data, "Message type resolver returned an unsupported resolution result.", null, headers)),
            };
        });
    }

    static RuntimeValueResult DeserializeValue(IValueSerializer serializer, Byte[] data, Type valueType, SerializationContext context, MessageHeaders headers, ValueFailureAction failureAction) {
        try {
            return RuntimeValueResult.Emit(new StreamValue.Resolved(serializer.Deserialize(data, valueType, context), valueType));
        }
        catch (Exception error) {
            return ApplyValueFailure(failureAction, new StreamValue.DeadLetter(null, data, $"Failed to deserialize value as CLR type '{valueType.FullName}'.", error, headers));
        }
    }

    static RuntimeValueResult ApplyValueFailure(ValueFailureAction action, StreamValue.DeadLetter deadLetter) => action switch {
        ValueFailureAction.SkipPolicy => RuntimeValueResult.Fail(RuntimeValueFailureAction.Skip),
        ValueFailureAction.ContinueAsDeadLetterPolicy => RuntimeValueResult.Emit(deadLetter),
        ValueFailureAction.PausePartitionPolicy => RuntimeValueResult.Fail(RuntimeValueFailureAction.PausePartition),
        _ => throw new InvalidOperationException("The requested value failure action is not supported by this runtime."),
    };
}
