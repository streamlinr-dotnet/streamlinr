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
    public async Task PeekCallbackExceptionFailsTopology() {
        var topic = $"streamlinr-processor-failure-{Guid.NewGuid():N}";
        await fixture.Kafka.CreateTopicAsync(topic, TestContext.Current.CancellationToken);

        var processorStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        using var stopStreamlinr = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token);

        var streamlinrTask = StreamlinrApplication.RunAsync(
            options: new StreamlinrOptions {
                ApplicationId    = $"streamlinr-processor-failure-{Guid.NewGuid():N}",
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
}
