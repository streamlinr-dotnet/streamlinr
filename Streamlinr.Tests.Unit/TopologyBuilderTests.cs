namespace Streamlinr;

using Xunit;

public sealed class TopologyBuilderTests {
    [Fact]
    public void StreamCanDeclarePeekProcessor() {
        var topology = new TopologyBuilder();

        topology.Stream<String, String>("orders")
            .Peek((_, _) => ValueTask.CompletedTask);
    }

    [Fact]
    public async Task RunRequiresApplicationId() {
        var options = new StreamlinrOptions { BootstrapServers = "localhost:9092" };

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => StreamlinrApplication.RunAsync(
            options,
            topology => topology.Stream<String, String>("orders").Peek((_, _) => ValueTask.CompletedTask),
            TestContext.Current.CancellationToken
        ));

        Assert.Contains("ApplicationId", error.Message);
    }

    [Fact]
    public async Task RunRequiresBootstrapServers() {
        var options = new StreamlinrOptions { ApplicationId = "orders-app" };

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => StreamlinrApplication.RunAsync(
            options,
            topology => topology.Stream<String, String>("orders").Peek((_, _) => ValueTask.CompletedTask),
            TestContext.Current.CancellationToken
        ));

        Assert.Contains("BootstrapServers", error.Message);
    }

    [Fact]
    public async Task InitialRuntimeRejectsNonStringStreams() {
        var options = new StreamlinrOptions { ApplicationId = "orders-app", BootstrapServers = "localhost:9092" };

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => StreamlinrApplication.RunAsync(
            options,
            topology => topology.Stream<Int32, String>("orders").Peek((_, _) => ValueTask.CompletedTask),
            TestContext.Current.CancellationToken
        ));

        Assert.Contains("string keys and string values", error.Message);
    }

    [Fact]
    public async Task InitialRuntimeRequiresProcessor() {
        var options = new StreamlinrOptions { ApplicationId = "orders-app", BootstrapServers = "localhost:9092" };

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => StreamlinrApplication.RunAsync(
            options,
            topology => topology.Stream<String, String>("orders"),
            TestContext.Current.CancellationToken
        ));

        Assert.Contains("processor", error.Message);
    }
}
