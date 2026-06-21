namespace Streamlinr;

using Xunit;

public sealed class MergeProcessorTests(KafkaIntegrationFixture fixture) : IClassFixture<KafkaIntegrationFixture> {
    [Fact]
    public async Task MergeReceivesRecordsFromBothSourceTopics() {
        var topicA = $"streamlinr-merge-a-{Guid.NewGuid():N}";
        var topicB = $"streamlinr-merge-b-{Guid.NewGuid():N}";
        await fixture.Kafka.CreateTopicAsync(topicA, TestContext.Current.CancellationToken);
        await fixture.Kafka.CreateTopicAsync(topicB, TestContext.Current.CancellationToken);

        var received = new List<StreamRecord<String>>();
        var receivedBoth = new TaskCompletionSource<IReadOnlyList<StreamRecord<String>>>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        using var stopStreamlinr = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token);

        var streamlinrTask = StreamlinrApplication.RunAsync(
            options: new StreamlinrOptions {
                ApplicationId    = $"streamlinr-merge-{Guid.NewGuid():N}",
                BootstrapServers = fixture.Kafka.BootstrapServers,
            },
            configureTopology: topology => {
                var sourceA = topology.Stream<String>(
                    topic              : topicA,
                    valueSerializer    : ValueSerializers.String,
                    messageTypeResolver: KafkaIntegrationFixture.StringResolver(),
                    failure            : ValueFailure.ContinueAsDeadLetter()
                );
                var sourceB = topology.Stream<String>(
                    topic              : topicB,
                    valueSerializer    : ValueSerializers.String,
                    messageTypeResolver: KafkaIntegrationFixture.StringResolver(),
                    failure            : ValueFailure.ContinueAsDeadLetter()
                );

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
                    }, ProcessorFailure.FailTopology());
            },
            cancellationToken: stopStreamlinr.Token);

        await fixture.ProduceStringAsync(topicA, "message-1", "value-1", TestContext.Current.CancellationToken);
        await fixture.ProduceStringAsync(topicB, "message-2", "value-2", TestContext.Current.CancellationToken);

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
        Assert.Contains(records, record => record is { Key: "message-1", Value: StreamValue.Resolved { Value: "value-1", Type: not null } });
        Assert.Contains(records, record => record is { Key: "message-2", Value: StreamValue.Resolved { Value: "value-2", Type: not null } });
    }
}
