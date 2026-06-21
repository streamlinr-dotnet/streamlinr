namespace Streamlinr;

using Xunit;

public sealed class SerializationTests(KafkaIntegrationFixture fixture) : IClassFixture<KafkaIntegrationFixture> {
    [Fact]
    public async Task StreamReceivesDeserializedWidget() {
        var topic = $"streamlinr-widget-{Guid.NewGuid():N}";
        await fixture.Kafka.CreateTopicAsync(topic, TestContext.Current.CancellationToken);

        var received = new TaskCompletionSource<StreamRecord<String>>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        using var stopStreamlinr = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token);

        var streamlinrTask = StreamlinrApplication.RunAsync(
            options: new StreamlinrOptions {
                ApplicationId    = $"streamlinr-widget-{Guid.NewGuid():N}",
                BootstrapServers = fixture.Kafka.BootstrapServers,
            },
            configureTopology: topology => {
                topology
                    .Stream<String>(
                        topic              : topic,
                        valueSerializer    : new KafkaIntegrationFixture.WidgetValueSerializer(),
                        messageTypeResolver: KafkaIntegrationFixture.WidgetResolver(),
                        failure            : ValueFailure.ContinueAsDeadLetter()
                    )
                    .Peek((record, _) => {
                        if (received.TrySetResult(record))
                            stopStreamlinr.Cancel();

                        return ValueTask.CompletedTask;
                    }, ProcessorFailure.FailTopology());
            },
            cancellationToken: stopStreamlinr.Token);

        var headers = new MessageHeaders {
            { "message-type", "widget"u8.ToArray() },
        };

        await fixture.Kafka.ProduceBytesAsync(
            topic,
            ("widget-1"u8.ToArray(), "widget-1:created"u8.ToArray()),
            headers,
            TestContext.Current.CancellationToken);

        var timeoutTask = Task.Delay(Timeout.InfiniteTimeSpan, timeout.Token);
        var completedTask = await Task.WhenAny(received.Task, streamlinrTask, timeoutTask);

        if (completedTask == streamlinrTask) {
            await streamlinrTask;
            Assert.Fail("Streamlinr runtime stopped before receiving the expected widget record.");
        }

        Assert.Same(received.Task, completedTask);

        await stopStreamlinr.CancelAsync();
        await streamlinrTask;

        var record = await received.Task;
        Assert.Equal("widget-1", record.Key);
        var value = Assert.IsType<StreamValue.Resolved>(record.Value);
        Assert.Equal(typeof(KafkaIntegrationFixture.Widget), value.Type);
        Assert.Equal(new KafkaIntegrationFixture.Widget("widget-1", "created"), value.Value);
    }

    [Fact]
    public async Task UnknownMessageTypeFlowsAsDeadLetterValue() {
        var topic = $"streamlinr-unknown-{Guid.NewGuid():N}";
        await fixture.Kafka.CreateTopicAsync(topic, TestContext.Current.CancellationToken);

        var received = new TaskCompletionSource<StreamRecord<String>>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        using var stopStreamlinr = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token);

        var streamlinrTask = StreamlinrApplication.RunAsync(
            options: new StreamlinrOptions {
                ApplicationId    = $"streamlinr-unknown-{Guid.NewGuid():N}",
                BootstrapServers = fixture.Kafka.BootstrapServers,
            },
            configureTopology: topology => {
                topology
                    .Stream<String>(
                        topic              : topic,
                        valueSerializer    : new KafkaIntegrationFixture.WidgetValueSerializer(),
                        messageTypeResolver: KafkaIntegrationFixture.WidgetResolver(),
                        failure            : ValueFailure.ContinueAsDeadLetter()
                    )
                    .Peek((record, _) => {
                        if (received.TrySetResult(record))
                            stopStreamlinr.Cancel();

                        return ValueTask.CompletedTask;
                    }, ProcessorFailure.FailTopology());
            },
            cancellationToken: stopStreamlinr.Token);

        var headers = new MessageHeaders {
            { "message-type", "unknown"u8.ToArray() },
        };
        await fixture.Kafka.ProduceBytesAsync(topic, ("widget-1"u8.ToArray(), "widget-1:created"u8.ToArray()), headers, TestContext.Current.CancellationToken);

        var timeoutTask = Task.Delay(Timeout.InfiniteTimeSpan, timeout.Token);
        var completedTask = await Task.WhenAny(received.Task, streamlinrTask, timeoutTask);

        if (completedTask == streamlinrTask) {
            await streamlinrTask;
            Assert.Fail("Streamlinr runtime stopped before receiving the dead-letter record.");
        }

        Assert.Same(received.Task, completedTask);

        await stopStreamlinr.CancelAsync();
        await streamlinrTask;

        var record = await received.Task;
        Assert.Equal("widget-1", record.Key);
        var value = Assert.IsType<StreamValue.DeadLetter>(record.Value);
        Assert.Contains("not mapped", value.Reason);
        Assert.IsType<Byte[]>(value.ValueData);
        Assert.Null(value.Error);
    }

    [Fact]
    public async Task NullMessageValueFlowsAsTombstone() {
        var topic = $"streamlinr-tombstone-{Guid.NewGuid():N}";
        await fixture.Kafka.CreateTopicAsync(topic, TestContext.Current.CancellationToken);

        var received = new TaskCompletionSource<StreamRecord<String>>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        using var stopStreamlinr = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token);

        var streamlinrTask = StreamlinrApplication.RunAsync(
            options: new StreamlinrOptions {
                ApplicationId    = $"streamlinr-tombstone-{Guid.NewGuid():N}",
                BootstrapServers = fixture.Kafka.BootstrapServers,
            },
            configureTopology: topology => {
                topology
                    .Stream<String>(
                        topic              : topic,
                        valueSerializer    : new KafkaIntegrationFixture.WidgetValueSerializer(),
                        messageTypeResolver: KafkaIntegrationFixture.WidgetResolver(),
                        failure            : ValueFailure.ContinueAsDeadLetter()
                    )
                    .Peek((record, _) => {
                        if (received.TrySetResult(record))
                            stopStreamlinr.Cancel();

                        return ValueTask.CompletedTask;
                    }, ProcessorFailure.FailTopology());
            },
            cancellationToken: stopStreamlinr.Token);

        await fixture.Kafka.ProduceBytesAsync(
            topic,
            ("widget-1"u8.ToArray(), null),
            new MessageHeaders(),
            TestContext.Current.CancellationToken);

        var timeoutTask = Task.Delay(Timeout.InfiniteTimeSpan, timeout.Token);
        var completedTask = await Task.WhenAny(received.Task, streamlinrTask, timeoutTask);

        if (completedTask == streamlinrTask) {
            await streamlinrTask;
            Assert.Fail("Streamlinr runtime stopped before receiving the tombstone record.");
        }

        Assert.Same(received.Task, completedTask);

        await stopStreamlinr.CancelAsync();
        await streamlinrTask;

        var record = await received.Task;
        Assert.Equal("widget-1", record.Key);
        Assert.IsType<StreamValue.Tombstone>(record.Value);
    }

    [Fact]
    public async Task BadPayloadFlowsAsDeadLetterValue() {
        var topic = $"streamlinr-bad-payload-{Guid.NewGuid():N}";
        await fixture.Kafka.CreateTopicAsync(topic, TestContext.Current.CancellationToken);

        var received = new TaskCompletionSource<StreamRecord<String>>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        using var stopStreamlinr = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token);

        var streamlinrTask = StreamlinrApplication.RunAsync(
            options: new StreamlinrOptions {
                ApplicationId    = $"streamlinr-bad-payload-{Guid.NewGuid():N}",
                BootstrapServers = fixture.Kafka.BootstrapServers,
            },
            configureTopology: topology => {
                topology
                    .Stream<String>(
                        topic              : topic,
                        valueSerializer    : new KafkaIntegrationFixture.WidgetValueSerializer(),
                        messageTypeResolver: KafkaIntegrationFixture.WidgetResolver(),
                        failure            : ValueFailure.ContinueAsDeadLetter()
                    )
                    .Peek((record, _) => {
                        if (received.TrySetResult(record))
                            stopStreamlinr.Cancel();

                        return ValueTask.CompletedTask;
                    }, ProcessorFailure.FailTopology());
            },
            cancellationToken: stopStreamlinr.Token);

        var headers = new MessageHeaders().Add("message-type", "widget"u8.ToArray());
        await fixture.Kafka.ProduceBytesAsync(topic, ("widget-1"u8.ToArray(), "not-a-widget"u8.ToArray()), headers, TestContext.Current.CancellationToken);

        var timeoutTask = Task.Delay(Timeout.InfiniteTimeSpan, timeout.Token);
        var completedTask = await Task.WhenAny(received.Task, streamlinrTask, timeoutTask);

        if (completedTask == streamlinrTask) {
            await streamlinrTask;
            Assert.Fail("Streamlinr runtime stopped before receiving the dead-letter record.");
        }

        Assert.Same(received.Task, completedTask);

        await stopStreamlinr.CancelAsync();
        await streamlinrTask;

        var record = await received.Task;
        Assert.Equal("widget-1", record.Key);
        var value = Assert.IsType<StreamValue.DeadLetter>(record.Value);
        Assert.IsType<Byte[]>(value.ValueData);
        Assert.Contains(typeof(KafkaIntegrationFixture.Widget).FullName!, value.Reason);
        Assert.IsType<InvalidOperationException>(value.Error);
    }

    [Fact]
    public async Task UnresolvedMessageTypeCanSkipWhileDeserializationFailureContinuesAsDeadLetter() {
        var topic = $"streamlinr-value-failure-split-{Guid.NewGuid():N}";
        await fixture.Kafka.CreateTopicAsync(topic, TestContext.Current.CancellationToken);

        var received = new TaskCompletionSource<StreamRecord<String>>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        using var stopStreamlinr = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token);

        var streamlinrTask = StreamlinrApplication.RunAsync(
            options: new StreamlinrOptions {
                ApplicationId    = $"streamlinr-value-failure-split-{Guid.NewGuid():N}",
                BootstrapServers = fixture.Kafka.BootstrapServers,
            },
            configureTopology: topology => {
                topology
                    .Stream<String>(
                        topic              : topic,
                        valueSerializer    : new KafkaIntegrationFixture.WidgetValueSerializer(),
                        messageTypeResolver: KafkaIntegrationFixture.WidgetResolver(),
                        failure            : ValueFailure.On(
                            unresolved           : ValueFailureAction.Skip(),
                            deserializationFailed: ValueFailureAction.ContinueAsDeadLetter())
                    )
                    .Peek((record, _) => {
                        if (received.TrySetResult(record))
                            stopStreamlinr.Cancel();

                        return ValueTask.CompletedTask;
                    }, ProcessorFailure.FailTopology());
            },
            cancellationToken: stopStreamlinr.Token);

        await fixture.Kafka.ProduceBytesAsync(
            topic,
            ("unknown-message"u8.ToArray(), "widget-1:created"u8.ToArray()),
            new MessageHeaders().Add("message-type", "unknown"u8.ToArray()),
            TestContext.Current.CancellationToken);

        await fixture.Kafka.ProduceBytesAsync(
            topic,
            ("bad-widget"u8.ToArray(), "not-a-widget"u8.ToArray()),
            new MessageHeaders().Add("message-type", "widget"u8.ToArray()),
            TestContext.Current.CancellationToken);

        var timeoutTask = Task.Delay(Timeout.InfiniteTimeSpan, timeout.Token);
        var completedTask = await Task.WhenAny(received.Task, streamlinrTask, timeoutTask);

        if (completedTask == streamlinrTask) {
            await streamlinrTask;
            Assert.Fail("Streamlinr runtime stopped before receiving the deserialization dead-letter record.");
        }

        Assert.Same(received.Task, completedTask);

        await stopStreamlinr.CancelAsync();
        await streamlinrTask;

        var record = await received.Task;
        Assert.Equal("bad-widget", record.Key);
        var value = Assert.IsType<StreamValue.DeadLetter>(record.Value);
        Assert.Contains(typeof(KafkaIntegrationFixture.Widget).FullName!, value.Reason);
        Assert.IsType<InvalidOperationException>(value.Error);
    }

    [Fact]
    public async Task DeserializationFailureCanPausePartition() {
        var topic = $"streamlinr-value-failure-pause-{Guid.NewGuid():N}";
        await fixture.Kafka.CreateTopicAsync(topic, partitionCount: 2, cancellationToken: TestContext.Current.CancellationToken);

        var partitionOneFirstReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var partitionOneSecondReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var receivedKeys = new List<String>();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        using var stopStreamlinr = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token);

        var streamlinrTask = StreamlinrApplication.RunAsync(
            options: new StreamlinrOptions {
                ApplicationId    = $"streamlinr-value-failure-pause-{Guid.NewGuid():N}",
                BootstrapServers = fixture.Kafka.BootstrapServers,
            },
            configureTopology: topology => {
                topology
                    .Stream<String>(
                        topic              : topic,
                        valueSerializer    : new KafkaIntegrationFixture.WidgetValueSerializer(),
                        messageTypeResolver: KafkaIntegrationFixture.WidgetResolver(),
                        failure            : ValueFailure.On(
                            unresolved           : ValueFailureAction.ContinueAsDeadLetter(),
                            deserializationFailed: ValueFailureAction.PausePartition())
                    )
                    .Peek((record, _) => {
                        lock (receivedKeys) {
                            receivedKeys.Add(record.Key);
                        }

                        if (record.Key == "partition-1-message-1")
                            partitionOneFirstReceived.TrySetResult();

                        if (record.Key == "partition-1-message-2")
                            partitionOneSecondReceived.TrySetResult();

                        return ValueTask.CompletedTask;
                    }, ProcessorFailure.FailTopology());
            },
            cancellationToken: stopStreamlinr.Token);

        var headers = new MessageHeaders().Add("message-type", "widget"u8.ToArray());
        await fixture.Kafka.ProduceBytesAsync(topic, 0, ("partition-0-message-1"u8.ToArray(), "not-a-widget"u8.ToArray()), headers, TestContext.Current.CancellationToken);
        await fixture.Kafka.ProduceBytesAsync(topic, 1, ("partition-1-message-1"u8.ToArray(), "widget-1:created"u8.ToArray()), headers, TestContext.Current.CancellationToken);

        var timeoutTask = Task.Delay(Timeout.InfiniteTimeSpan, timeout.Token);
        Assert.Same(partitionOneFirstReceived.Task, await Task.WhenAny(partitionOneFirstReceived.Task, streamlinrTask, timeoutTask));

        await fixture.Kafka.ProduceBytesAsync(topic, 0, ("partition-0-message-2"u8.ToArray(), "widget-2:created"u8.ToArray()), headers, TestContext.Current.CancellationToken);
        await fixture.Kafka.ProduceBytesAsync(topic, 1, ("partition-1-message-2"u8.ToArray(), "widget-3:created"u8.ToArray()), headers, TestContext.Current.CancellationToken);

        Assert.Same(partitionOneSecondReceived.Task, await Task.WhenAny(partitionOneSecondReceived.Task, streamlinrTask, timeoutTask));

        var settleTask = Task.Delay(TimeSpan.FromMilliseconds(500), timeout.Token);
        Assert.Same(settleTask, await Task.WhenAny(streamlinrTask, settleTask));
        Assert.False(streamlinrTask.IsCompleted);

        lock (receivedKeys) {
            Assert.DoesNotContain("partition-0-message-1", receivedKeys);
            Assert.Contains("partition-1-message-1", receivedKeys);
            Assert.DoesNotContain("partition-0-message-2", receivedKeys);
            Assert.Contains("partition-1-message-2", receivedKeys);
        }

        await stopStreamlinr.CancelAsync();
        await streamlinrTask;
    }
}
