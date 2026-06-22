namespace Streamlinr;

using Xunit;

public sealed class OffsetCommitTests(KafkaIntegrationFixture fixture) : IClassFixture<KafkaIntegrationFixture> {
    [Fact]
    public async Task SuccessfulProcessorCommitsSourceOffset() {
        var topic = $"streamlinr-commit-success-{Guid.NewGuid():N}";
        var applicationId = $"streamlinr-commit-success-{Guid.NewGuid():N}";
        await fixture.Kafka.CreateTopicAsync(topic, TestContext.Current.CancellationToken);

        var processed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        using var stopStreamlinr = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token);

        var streamlinrTask = StreamlinrApplication.RunAsync(
            options: new StreamlinrOptions {
                ApplicationId    = applicationId,
                BootstrapServers = fixture.Kafka.BootstrapServers,
            },
            configureTopology: topology => {
                topology
                    .Stream<String>(topic, ValueSerializers.String, KafkaIntegrationFixture.StringResolver(), ValueFailure.ContinueAsDeadLetter())
                    .Peek((_, _) => {
                        processed.TrySetResult();
                        return ValueTask.CompletedTask;
                    }, ProcessorFailure.FailTopology());
            },
            cancellationToken: stopStreamlinr.Token);

        await fixture.ProduceStringAsync(topic, "message-1", "value-1", TestContext.Current.CancellationToken);

        await processed.Task.WaitAsync(timeout.Token);
        await fixture.Kafka.WaitForCommittedOffsetAsync(topic, applicationId, partition: 0, expectedOffset: 1, timeout.Token);

        await stopStreamlinr.CancelAsync();
        await streamlinrTask;
    }

    [Fact]
    public async Task ProcessorSkipCommitsSourceOffset() {
        var topic = $"streamlinr-commit-skip-{Guid.NewGuid():N}";
        var applicationId = $"streamlinr-commit-skip-{Guid.NewGuid():N}";
        await fixture.Kafka.CreateTopicAsync(topic, TestContext.Current.CancellationToken);

        var processed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        using var stopStreamlinr = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token);

        var streamlinrTask = StreamlinrApplication.RunAsync(
            options: new StreamlinrOptions {
                ApplicationId    = applicationId,
                BootstrapServers = fixture.Kafka.BootstrapServers,
            },
            configureTopology: topology => {
                topology
                    .Stream<String>(topic, ValueSerializers.String, KafkaIntegrationFixture.StringResolver(), ValueFailure.ContinueAsDeadLetter())
                    .Peek((_, _) => {
                        processed.TrySetResult();
                        throw new InvalidOperationException("processor failed");
                    }, ProcessorFailure.Skip());
            },
            cancellationToken: stopStreamlinr.Token);

        await fixture.ProduceStringAsync(topic, "message-1", "value-1", TestContext.Current.CancellationToken);

        await processed.Task.WaitAsync(timeout.Token);
        await fixture.Kafka.WaitForCommittedOffsetAsync(topic, applicationId, partition: 0, expectedOffset: 1, timeout.Token);

        await stopStreamlinr.CancelAsync();
        await streamlinrTask;
    }

    [Fact]
    public async Task ProcessorFailureDoesNotCommitSourceOffset() {
        var topic = $"streamlinr-no-commit-failure-{Guid.NewGuid():N}";
        var applicationId = $"streamlinr-no-commit-failure-{Guid.NewGuid():N}";
        await fixture.Kafka.CreateTopicAsync(topic, TestContext.Current.CancellationToken);

        var processed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        using var stopStreamlinr = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token);

        var streamlinrTask = StreamlinrApplication.RunAsync(
            options: new StreamlinrOptions {
                ApplicationId    = applicationId,
                BootstrapServers = fixture.Kafka.BootstrapServers,
            },
            configureTopology: topology => {
                topology
                    .Stream<String>(topic, ValueSerializers.String, KafkaIntegrationFixture.StringResolver(), ValueFailure.ContinueAsDeadLetter())
                    .Peek((_, _) => {
                        processed.TrySetResult();
                        throw new InvalidOperationException("processor failed");
                    }, ProcessorFailure.FailTopology());
            },
            cancellationToken: stopStreamlinr.Token);

        await fixture.ProduceStringAsync(topic, "message-1", "value-1", TestContext.Current.CancellationToken);

        await processed.Task.WaitAsync(timeout.Token);
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await streamlinrTask.WaitAsync(timeout.Token));

        Assert.Null(fixture.Kafka.GetCommittedOffset(topic, applicationId, partition: 0));
    }
}
