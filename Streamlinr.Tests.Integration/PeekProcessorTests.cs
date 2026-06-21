namespace Streamlinr;

using Xunit;

public sealed class PeekProcessorTests(KafkaIntegrationFixture fixture) : IClassFixture<KafkaIntegrationFixture> {
    [Fact]
    public async Task PeekReceivesProducedKafkaRecord() {
        var topic = $"streamlinr-smoke-{Guid.NewGuid():N}";
        await fixture.Kafka.CreateTopicAsync(topic, TestContext.Current.CancellationToken);

        var received = new TaskCompletionSource<StreamRecord<String>>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        using var stopStreamlinr = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token);

        var streamlinrTask = StreamlinrApplication.RunAsync(
            options: new StreamlinrOptions {
                ApplicationId    = $"streamlinr-smoke-{Guid.NewGuid():N}",
                BootstrapServers = fixture.Kafka.BootstrapServers,
            },
            configureTopology: topology => {
                topology
                    .Stream<String>(
                        topic              : topic,
                        valueSerializer    : ValueSerializers.String,
                        messageTypeResolver: KafkaIntegrationFixture.StringResolver(),
                        failure            : ValueFailure.ContinueAsDeadLetter()
                    )
                    .Peek((record, _) => {
                        if (received.TrySetResult(record))
                            stopStreamlinr.Cancel();

                        return ValueTask.CompletedTask;
                    }, ProcessorFailure.FailTopology());
            },
            cancellationToken: stopStreamlinr.Token);

        await fixture.ProduceStringAsync(topic, "order-1", "created", TestContext.Current.CancellationToken);

        var timeoutTask = Task.Delay(Timeout.InfiniteTimeSpan, timeout.Token);
        var completedTask = await Task.WhenAny(received.Task, streamlinrTask, timeoutTask);

        if (completedTask == streamlinrTask) {
            await streamlinrTask;
            Assert.Fail("Streamlinr runtime stopped before receiving the expected record.");
        }

        Assert.Same(received.Task, completedTask);

        await stopStreamlinr.CancelAsync();
        await streamlinrTask;

        var record = await received.Task;
        Assert.Equal("order-1", record.Key);
        var value = Assert.IsType<StreamValue.Resolved>(record.Value);
        Assert.Equal(typeof(String), value.Type);
        Assert.Equal("created", value.Value);
    }

    [Fact]
    public async Task PeekCallbackExceptionCanFailTopology() {
        var topic = $"streamlinr-processor-fail-topology-{Guid.NewGuid():N}";
        await fixture.Kafka.CreateTopicAsync(topic, TestContext.Current.CancellationToken);

        var processorStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        using var stopStreamlinr = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token);

        var streamlinrTask = StreamlinrApplication.RunAsync(
            options: new StreamlinrOptions {
                ApplicationId    = $"streamlinr-processor-fail-topology-{Guid.NewGuid():N}",
                BootstrapServers = fixture.Kafka.BootstrapServers,
            },
            configureTopology: topology => {
                topology
                    .Stream<String>(
                        topic              : topic,
                        valueSerializer    : ValueSerializers.String,
                        messageTypeResolver: KafkaIntegrationFixture.StringResolver(),
                        failure            : ValueFailure.ContinueAsDeadLetter()
                    )
                    .Peek((_, _) => {
                        processorStarted.TrySetResult();
                        throw new InvalidOperationException("processor failed");
                    }, ProcessorFailure.FailTopology());
            },
            cancellationToken: stopStreamlinr.Token);

        await fixture.ProduceStringAsync(topic, "message-1", "value-1", TestContext.Current.CancellationToken);

        var timeoutTask = Task.Delay(Timeout.InfiniteTimeSpan, timeout.Token);
        var completedTask = await Task.WhenAny(streamlinrTask, timeoutTask);

        if (completedTask != streamlinrTask) {
            await stopStreamlinr.CancelAsync();
            Assert.Fail("Streamlinr runtime did not stop after the processor failed.");
        }

        await processorStarted.Task;
        var error = await Assert.ThrowsAsync<InvalidOperationException>(async () => await streamlinrTask);
        Assert.Equal("processor failed", error.Message);
    }

    [Fact]
    public async Task PeekCallbackExceptionCanSkipRecord() {
        var topic = $"streamlinr-processor-skip-{Guid.NewGuid():N}";
        await fixture.Kafka.CreateTopicAsync(topic, TestContext.Current.CancellationToken);

        var firstProcessorStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var downstreamReceived = new TaskCompletionSource<StreamRecord<String>>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        using var stopStreamlinr = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token);

        var streamlinrTask = StreamlinrApplication.RunAsync(
            options: new StreamlinrOptions {
                ApplicationId    = $"streamlinr-processor-skip-{Guid.NewGuid():N}",
                BootstrapServers = fixture.Kafka.BootstrapServers,
            },
            configureTopology: topology => {
                topology
                    .Stream<String>(
                        topic              : topic,
                        valueSerializer    : ValueSerializers.String,
                        messageTypeResolver: KafkaIntegrationFixture.StringResolver(),
                        failure            : ValueFailure.ContinueAsDeadLetter()
                    )
                    .Peek((_, _) => {
                        firstProcessorStarted.TrySetResult();
                        throw new InvalidOperationException("processor failed");
                    }, ProcessorFailure.Skip())
                    .Peek((record, _) => {
                        downstreamReceived.TrySetResult(record);
                        return ValueTask.CompletedTask;
                    }, ProcessorFailure.FailTopology());
            },
            cancellationToken: stopStreamlinr.Token);

        await fixture.ProduceStringAsync(topic, "message-1", "value-1", TestContext.Current.CancellationToken);

        var timeoutTask = Task.Delay(Timeout.InfiniteTimeSpan, timeout.Token);
        Assert.Same(firstProcessorStarted.Task, await Task.WhenAny(firstProcessorStarted.Task, streamlinrTask, timeoutTask));
        var settleTask = Task.Delay(TimeSpan.FromMilliseconds(500), timeout.Token);
        Assert.Same(settleTask, await Task.WhenAny(downstreamReceived.Task, streamlinrTask, settleTask));
        Assert.False(streamlinrTask.IsCompleted);

        await stopStreamlinr.CancelAsync();
        await streamlinrTask;
    }

    [Fact]
    public async Task PeekCallbackExceptionCanContinueAsDeadLetter() {
        var topic = $"streamlinr-processor-dead-letter-{Guid.NewGuid():N}";
        await fixture.Kafka.CreateTopicAsync(topic, TestContext.Current.CancellationToken);

        var downstreamReceived = new TaskCompletionSource<StreamRecord<String>>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        using var stopStreamlinr = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token);

        var streamlinrTask = StreamlinrApplication.RunAsync(
            options: new StreamlinrOptions {
                ApplicationId    = $"streamlinr-processor-dead-letter-{Guid.NewGuid():N}",
                BootstrapServers = fixture.Kafka.BootstrapServers,
            },
            configureTopology: topology => {
                topology
                    .Stream<String>(
                        topic              : topic,
                        valueSerializer    : ValueSerializers.String,
                        messageTypeResolver: KafkaIntegrationFixture.StringResolver(),
                        failure            : ValueFailure.ContinueAsDeadLetter()
                    )
                    .Peek((_, _) => throw new InvalidOperationException("processor failed"), ProcessorFailure.ContinueAsDeadLetter())
                    .Peek((record, _) => {
                        downstreamReceived.TrySetResult(record);
                        stopStreamlinr.Cancel();
                        return ValueTask.CompletedTask;
                    }, ProcessorFailure.FailTopology());
            },
            cancellationToken: stopStreamlinr.Token);

        await fixture.ProduceStringAsync(topic, "message-1", "value-1", TestContext.Current.CancellationToken);

        var timeoutTask = Task.Delay(Timeout.InfiniteTimeSpan, timeout.Token);
        var completedTask = await Task.WhenAny(downstreamReceived.Task, streamlinrTask, timeoutTask);

        if (completedTask == streamlinrTask) {
            await streamlinrTask;
            Assert.Fail("Streamlinr runtime stopped before receiving the dead-letter record.");
        }

        Assert.Same(downstreamReceived.Task, completedTask);

        await stopStreamlinr.CancelAsync();
        await streamlinrTask;

        var record = await downstreamReceived.Task;
        Assert.Equal("message-1", record.Key);
        var value = Assert.IsType<StreamValue.DeadLetter>(record.Value);
        Assert.Equal("message-1", value.KeyData);
        Assert.IsType<StreamValue.Resolved>(value.ValueData);
        Assert.Contains("partition", value.Reason);
        Assert.IsType<InvalidOperationException>(value.Error);
    }

    [Fact]
    public async Task PeekCallbackExceptionCanPausePartition() {
        var topic = $"streamlinr-processor-pause-{Guid.NewGuid():N}";
        await fixture.Kafka.CreateTopicAsync(topic, partitionCount: 2, cancellationToken: TestContext.Current.CancellationToken);

        var partitionPaused = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var partitionOneFirstReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var partitionOneSecondReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstProcessorKeys = new List<String>();
        var downstreamKeys = new List<String>();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        using var stopStreamlinr = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token);

        var streamlinrTask = StreamlinrApplication.RunAsync(
            options: new StreamlinrOptions {
                ApplicationId    = $"streamlinr-processor-pause-{Guid.NewGuid():N}",
                BootstrapServers = fixture.Kafka.BootstrapServers,
            },
            configureTopology: topology => {
                topology
                    .Stream<String>(
                        topic              : topic,
                        valueSerializer    : ValueSerializers.String,
                        messageTypeResolver: KafkaIntegrationFixture.StringResolver(),
                        failure            : ValueFailure.ContinueAsDeadLetter()
                    )
                    .Peek((record, _) => {
                        lock (firstProcessorKeys) {
                            firstProcessorKeys.Add(record.Key);
                        }

                        if (record.Key == "partition-0-message-1") {
                            partitionPaused.TrySetResult();
                            throw new InvalidOperationException("processor failed");
                        }

                        return ValueTask.CompletedTask;
                    }, ProcessorFailure.PausePartition())
                    .Peek((record, _) => {
                        lock (downstreamKeys) {
                            downstreamKeys.Add(record.Key);
                        }

                        if (record.Key == "partition-1-message-1")
                            partitionOneFirstReceived.TrySetResult();

                        if (record.Key == "partition-1-message-2")
                            partitionOneSecondReceived.TrySetResult();

                        return ValueTask.CompletedTask;
                    }, ProcessorFailure.FailTopology());
            },
            cancellationToken: stopStreamlinr.Token);

        var headers = new MessageHeaders {
            { "message-type", "string"u8.ToArray() },
        };

        await fixture.Kafka.ProduceBytesAsync(topic, 0, ("partition-0-message-1"u8.ToArray(), "value-1"u8.ToArray()), headers, TestContext.Current.CancellationToken);
        await fixture.Kafka.ProduceBytesAsync(topic, 1, ("partition-1-message-1"u8.ToArray(), "value-1"u8.ToArray()), headers, TestContext.Current.CancellationToken);

        var timeoutTask = Task.Delay(Timeout.InfiniteTimeSpan, timeout.Token);
        Assert.Same(partitionPaused.Task, await Task.WhenAny(partitionPaused.Task, streamlinrTask, timeoutTask));
        Assert.Same(partitionOneFirstReceived.Task, await Task.WhenAny(partitionOneFirstReceived.Task, streamlinrTask, timeoutTask));

        await fixture.Kafka.ProduceBytesAsync(topic, 0, ("partition-0-message-2"u8.ToArray(), "value-2"u8.ToArray()), headers, TestContext.Current.CancellationToken);
        await fixture.Kafka.ProduceBytesAsync(topic, 1, ("partition-1-message-2"u8.ToArray(), "value-2"u8.ToArray()), headers, TestContext.Current.CancellationToken);

        Assert.Same(partitionOneSecondReceived.Task, await Task.WhenAny(partitionOneSecondReceived.Task, streamlinrTask, timeoutTask));

        var settleTask = Task.Delay(TimeSpan.FromMilliseconds(500), timeout.Token);
        Assert.Same(settleTask, await Task.WhenAny(streamlinrTask, settleTask));
        Assert.False(streamlinrTask.IsCompleted);

        lock (firstProcessorKeys) {
            Assert.Contains("partition-0-message-1", firstProcessorKeys);
            Assert.Contains("partition-1-message-1", firstProcessorKeys);
            Assert.DoesNotContain("partition-0-message-2", firstProcessorKeys);
            Assert.Contains("partition-1-message-2", firstProcessorKeys);
        }

        lock (downstreamKeys) {
            Assert.DoesNotContain("partition-0-message-1", downstreamKeys);
            Assert.Contains("partition-1-message-1", downstreamKeys);
            Assert.DoesNotContain("partition-0-message-2", downstreamKeys);
            Assert.Contains("partition-1-message-2", downstreamKeys);
        }

        await stopStreamlinr.CancelAsync();
        await streamlinrTask;
    }
}
