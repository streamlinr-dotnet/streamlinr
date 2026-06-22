namespace Streamlinr;

using static System.Text.Encoding;

using Xunit;

public sealed class ToTopicSinkTests(KafkaIntegrationFixture fixture) : IClassFixture<KafkaIntegrationFixture> {
    [Fact]
    public async Task ToTopicProducesResolvedRecord() {
        var inputTopic = $"streamlinr-to-input-{Guid.NewGuid():N}";
        var outputTopic = $"streamlinr-to-output-{Guid.NewGuid():N}";
        await fixture.Kafka.CreateTopicAsync(inputTopic, TestContext.Current.CancellationToken);
        await fixture.Kafka.CreateTopicAsync(outputTopic, TestContext.Current.CancellationToken);

        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        using var stopStreamlinr = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token);

        var streamlinrTask = StreamlinrApplication.RunAsync(
            options: new StreamlinrOptions {
                ApplicationId = $"streamlinr-to-topic-{Guid.NewGuid():N}",
                BootstrapServers = fixture.Kafka.BootstrapServers,
            },
            configureTopology: topology => {
                topology
                    .Stream<String>(inputTopic, ValueSerializers.String, KafkaIntegrationFixture.StringResolver(), ValueFailure.ContinueAsDeadLetter())
                    .ToTopic(outputTopic, ValueSerializers.String, KafkaIntegrationFixture.StringResolver(), DeadLetterHandling.Fail(), ProcessorFailure.FailTopology());
            },
            cancellationToken: stopStreamlinr.Token);

        await fixture.ProduceStringAsync(inputTopic, "message-1", "value-1", TestContext.Current.CancellationToken);

        var outputTask = fixture.Kafka.ConsumeBytesAsync(outputTopic, $"streamlinr-to-topic-reader-{Guid.NewGuid():N}", timeout.Token);
        var completedTask = await Task.WhenAny(outputTask, streamlinrTask, Task.Delay(Timeout.InfiniteTimeSpan, timeout.Token));

        if (completedTask == streamlinrTask) {
            await streamlinrTask;
            Assert.Fail("Streamlinr runtime stopped before producing the output record.");
        }

        Assert.Same(outputTask, completedTask);

        await stopStreamlinr.CancelAsync();
        await streamlinrTask;

        var output = await outputTask;
        Assert.Equal("message-1", UTF8.GetString(output.Key));
        Assert.Equal("value-1", UTF8.GetString(output.Value!));
        Assert.True(output.Headers.TryGetLast("message-type", out var messageType));
        Assert.Equal("string", UTF8.GetString(messageType));
    }

    [Fact]
    public async Task ToTopicProducesTombstone() {
        var inputTopic = $"streamlinr-to-tombstone-input-{Guid.NewGuid():N}";
        var outputTopic = $"streamlinr-to-tombstone-output-{Guid.NewGuid():N}";
        await fixture.Kafka.CreateTopicAsync(inputTopic, TestContext.Current.CancellationToken);
        await fixture.Kafka.CreateTopicAsync(outputTopic, TestContext.Current.CancellationToken);

        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        using var stopStreamlinr = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token);

        var streamlinrTask = StreamlinrApplication.RunAsync(
            options: new StreamlinrOptions {
                ApplicationId = $"streamlinr-to-tombstone-{Guid.NewGuid():N}",
                BootstrapServers = fixture.Kafka.BootstrapServers,
            },
            configureTopology: topology => {
                topology
                    .Stream<String>(inputTopic, ValueSerializers.String, KafkaIntegrationFixture.StringResolver(), ValueFailure.ContinueAsDeadLetter())
                    .ToTopic(outputTopic, ValueSerializers.String, KafkaIntegrationFixture.StringResolver(), DeadLetterHandling.Fail(), ProcessorFailure.FailTopology());
            },
            cancellationToken: stopStreamlinr.Token);

        await fixture.Kafka.ProduceBytesAsync(
            inputTopic,
            ("message-1"u8.ToArray(), null),
            new MessageHeaders(),
            TestContext.Current.CancellationToken);

        var outputTask = fixture.Kafka.ConsumeBytesAsync(outputTopic, $"streamlinr-to-tombstone-reader-{Guid.NewGuid():N}", timeout.Token);
        var completedTask = await Task.WhenAny(outputTask, streamlinrTask, Task.Delay(Timeout.InfiniteTimeSpan, timeout.Token));

        if (completedTask == streamlinrTask) {
            await streamlinrTask;
            Assert.Fail("Streamlinr runtime stopped before producing the tombstone output record.");
        }

        Assert.Same(outputTask, completedTask);

        await stopStreamlinr.CancelAsync();
        await streamlinrTask;

        var output = await outputTask;
        Assert.Equal("message-1", UTF8.GetString(output.Key));
        Assert.Null(output.Value);
    }

    [Fact]
    public async Task ToTopicSkipsDeadLetterWhenConfigured() {
        var inputTopic = $"streamlinr-to-skip-dead-input-{Guid.NewGuid():N}";
        var outputTopic = $"streamlinr-to-skip-dead-output-{Guid.NewGuid():N}";
        await fixture.Kafka.CreateTopicAsync(inputTopic, TestContext.Current.CancellationToken);
        await fixture.Kafka.CreateTopicAsync(outputTopic, TestContext.Current.CancellationToken);

        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        using var stopStreamlinr = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token);

        var streamlinrTask = StreamlinrApplication.RunAsync(
            options: new StreamlinrOptions {
                ApplicationId = $"streamlinr-to-skip-dead-{Guid.NewGuid():N}",
                BootstrapServers = fixture.Kafka.BootstrapServers,
            },
            configureTopology: topology => {
                topology
                    .Stream<String>(inputTopic, ValueSerializers.String, KafkaIntegrationFixture.StringResolver(), ValueFailure.ContinueAsDeadLetter())
                    .ToTopic(outputTopic, ValueSerializers.String, KafkaIntegrationFixture.StringResolver(), DeadLetterHandling.Skip(), ProcessorFailure.FailTopology());
            },
            cancellationToken: stopStreamlinr.Token);

        var unknownHeaders = new MessageHeaders().Add("message-type", "unknown"u8.ToArray());
        await fixture.Kafka.ProduceBytesAsync(inputTopic, ("dead-letter"u8.ToArray(), "value-1"u8.ToArray()), unknownHeaders, TestContext.Current.CancellationToken);
        await fixture.ProduceStringAsync(inputTopic, "message-2", "value-2", TestContext.Current.CancellationToken);

        var outputTask = fixture.Kafka.ConsumeBytesAsync(outputTopic, $"streamlinr-to-skip-dead-reader-{Guid.NewGuid():N}", timeout.Token);
        var completedTask = await Task.WhenAny(outputTask, streamlinrTask, Task.Delay(Timeout.InfiniteTimeSpan, timeout.Token));

        if (completedTask == streamlinrTask) {
            await streamlinrTask;
            Assert.Fail("Streamlinr runtime stopped before producing the non-dead-letter output record.");
        }

        Assert.Same(outputTask, completedTask);

        await stopStreamlinr.CancelAsync();
        await streamlinrTask;

        var output = await outputTask;
        Assert.Equal("message-2", UTF8.GetString(output.Key));
        Assert.Equal("value-2", UTF8.GetString(output.Value!));
    }

    [Fact]
    public async Task ToTopicFailsTopologyWhenDeadLetterHandlingFails() {
        var inputTopic = $"streamlinr-to-fail-dead-input-{Guid.NewGuid():N}";
        var outputTopic = $"streamlinr-to-fail-dead-output-{Guid.NewGuid():N}";
        await fixture.Kafka.CreateTopicAsync(inputTopic, TestContext.Current.CancellationToken);
        await fixture.Kafka.CreateTopicAsync(outputTopic, TestContext.Current.CancellationToken);

        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        using var stopStreamlinr = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token);

        var streamlinrTask = StreamlinrApplication.RunAsync(
            options: new StreamlinrOptions {
                ApplicationId = $"streamlinr-to-fail-dead-{Guid.NewGuid():N}",
                BootstrapServers = fixture.Kafka.BootstrapServers,
            },
            configureTopology: topology => {
                topology
                    .Stream<String>(inputTopic, ValueSerializers.String, KafkaIntegrationFixture.StringResolver(), ValueFailure.ContinueAsDeadLetter())
                    .ToTopic(outputTopic, ValueSerializers.String, KafkaIntegrationFixture.StringResolver(), DeadLetterHandling.Fail(), ProcessorFailure.FailTopology());
            },
            cancellationToken: stopStreamlinr.Token);

        var unknownHeaders = new MessageHeaders().Add("message-type", "unknown"u8.ToArray());
        await fixture.Kafka.ProduceBytesAsync(inputTopic, ("dead-letter"u8.ToArray(), "value-1"u8.ToArray()), unknownHeaders, TestContext.Current.CancellationToken);

        var completedTask = await Task.WhenAny(streamlinrTask, Task.Delay(Timeout.InfiniteTimeSpan, timeout.Token));

        if (completedTask != streamlinrTask) {
            await stopStreamlinr.CancelAsync();
            Assert.Fail("Streamlinr runtime did not stop after a dead-letter value reached a fail-fast topic sink.");
        }

        var error = await Assert.ThrowsAsync<InvalidOperationException>(async () => await streamlinrTask);
        Assert.Contains("Dead-letter value reached topic sink", error.Message);
    }
}
