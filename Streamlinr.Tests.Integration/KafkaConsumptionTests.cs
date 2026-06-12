namespace Streamlinr;

using Xunit;

public sealed class KafkaConsumptionTests : IAsyncLifetime {
    readonly KafkaTestContainer _kafka = new KafkaTestContainer();

    public async ValueTask InitializeAsync() {
        await _kafka.StartAsync(TimeSpan.FromSeconds(60));
    }

    public async ValueTask DisposeAsync() => await _kafka.DisposeAsync();

    [Fact]
    public async Task PeekReceivesProducedKafkaRecord() {
        var topic = $"streamlinr-smoke-{Guid.NewGuid():N}";
        await _kafka.CreateTopicAsync(topic, TestContext.Current.CancellationToken);

        var received = new TaskCompletionSource<StreamRecord<String, String>>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        using var stopStreamlinr = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token);

        var streamlinrTask = StreamlinrApplication.RunAsync(
            options: new StreamlinrOptions {
                ApplicationId    = $"streamlinr-smoke-{Guid.NewGuid():N}",
                BootstrapServers = _kafka.BootstrapServers,
            },
            configureTopology: topology => {
                topology
                    .Stream<String, String>(topic)
                    .Peek((record, _) => {
                        if (received.TrySetResult(record))
                            stopStreamlinr.Cancel();

                        return ValueTask.CompletedTask;
                    });
            },
            cancellationToken: stopStreamlinr.Token);

        await _kafka.ProduceAsync(topic, ("order-1", "created"), TestContext.Current.CancellationToken);

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
        Assert.Equal("created", record.Value);
    }
}
