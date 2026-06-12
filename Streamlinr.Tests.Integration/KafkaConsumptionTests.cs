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

    [Fact]
    public async Task MergeReceivesRecordsFromBothSourceTopics() {
        var topicA = $"streamlinr-merge-a-{Guid.NewGuid():N}";
        var topicB = $"streamlinr-merge-b-{Guid.NewGuid():N}";
        await _kafka.CreateTopicAsync(topicA, TestContext.Current.CancellationToken);
        await _kafka.CreateTopicAsync(topicB, TestContext.Current.CancellationToken);

        var received = new List<StreamRecord<String, String>>();
        var receivedBoth = new TaskCompletionSource<IReadOnlyList<StreamRecord<String, String>>>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        using var stopStreamlinr = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token);

        var streamlinrTask = StreamlinrApplication.RunAsync(
            options: new StreamlinrOptions {
                ApplicationId    = $"streamlinr-merge-{Guid.NewGuid():N}",
                BootstrapServers = _kafka.BootstrapServers,
            },
            configureTopology: topology => {
                var sourceA = topology.Stream<String, String>(topicA);
                var sourceB = topology.Stream<String, String>(topicB);

                topology
                    .Merge(sourceA, sourceB)
                    .Peek((record, _) => {
                        lock (received) {
                            received.Add(record);

                            if (received.Count == 2) {
                                receivedBoth.TrySetResult(received.ToArray());
                                stopStreamlinr.Cancel();
                            }
                        }

                        return ValueTask.CompletedTask;
                    });
            },
            cancellationToken: stopStreamlinr.Token);

        await _kafka.ProduceAsync(topicA, ("message-1", "value-1"), TestContext.Current.CancellationToken);
        await _kafka.ProduceAsync(topicB, ("message-2", "value-2"), TestContext.Current.CancellationToken);

        var timeoutTask = Task.Delay(Timeout.InfiniteTimeSpan, timeout.Token);
        var completedTask = await Task.WhenAny(receivedBoth.Task, streamlinrTask, timeoutTask);

        if (completedTask == streamlinrTask) {
            await streamlinrTask;
            Assert.Fail("Streamlinr runtime stopped before receiving both merged records.");
        }

        Assert.Same(receivedBoth.Task, completedTask);

        await stopStreamlinr.CancelAsync();
        await streamlinrTask;

        var records = await receivedBoth.Task;
        Assert.Contains(records, record => record is { Key: "message-1", Value: "value-1" });
        Assert.Contains(records, record => record is { Key: "message-2", Value: "value-2" });
    }
}
