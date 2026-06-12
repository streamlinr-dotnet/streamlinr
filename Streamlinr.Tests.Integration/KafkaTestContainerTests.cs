namespace Streamlinr;

using Xunit;

[Collection(TestInfrastructureCollection.Name)]
public sealed class KafkaTestContainerTests : IAsyncLifetime {
    readonly KafkaTestContainer _kafka = new KafkaTestContainer();

    public async ValueTask InitializeAsync() {
        await _kafka.StartAsync(TimeSpan.FromSeconds(60));
    }

    public async ValueTask DisposeAsync() => await _kafka.DisposeAsync();

    [Fact]
    public async Task HelperOperationsBypassKafkaNetworkFailures() {
        var topic = $"streamlinr-helper-{Guid.NewGuid():N}";

        await _kafka.DisconnectKafkaAsync(TestContext.Current.CancellationToken);

        var exception = await Record.ExceptionAsync(async () => {
            await _kafka.CreateTopicAsync(topic, TestContext.Current.CancellationToken);
            await _kafka.ProduceAsync(topic, ("order-1", "created"), TestContext.Current.CancellationToken);
        });

        await _kafka.RestoreNetworkAsync(TestContext.Current.CancellationToken);

        Assert.Null(exception);
    }
}
