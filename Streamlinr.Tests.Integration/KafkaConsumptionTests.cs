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
        using var stopRuntime = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token);

        var runTask = StreamlinrApplication.RunAsync(
            options: new StreamlinrOptions {
                ApplicationId    = $"streamlinr-smoke-{Guid.NewGuid():N}",
                BootstrapServers = _kafka.BootstrapServers,
            },
            configureTopology: topology => {
                topology
                    .Stream<String, String>(topic)
                    .Peek((record, _) => {
                        if (received.TrySetResult(record))
                            stopRuntime.Cancel();

                        return ValueTask.CompletedTask;
                    });
            },
            cancellationToken: stopRuntime.Token);

        await Task.Delay(TimeSpan.FromSeconds(3), TestContext.Current.CancellationToken);
        await _kafka.ProduceAsync(topic, ("order-1", "created"), TestContext.Current.CancellationToken);

        var delay = Task.Delay(Timeout.InfiniteTimeSpan, timeout.Token);
        var completed = await Task.WhenAny(received.Task, runTask, delay);

        if (completed == runTask) {
            await runTask;
            Assert.Fail("Streamlinr runtime stopped before receiving the expected record.");
        }

        Assert.Same(received.Task, completed);

        await stopRuntime.CancelAsync();
        await runTask;

        var record = await received.Task;
        Assert.Equal("order-1", record.Key);
        Assert.Equal("created", record.Value);
    }
}
