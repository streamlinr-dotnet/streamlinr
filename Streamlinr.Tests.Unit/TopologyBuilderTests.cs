namespace Streamlinr;

using Xunit;

public sealed class TopologyBuilderTests {
    [Fact]
    public void StreamCanDeclarePeekProcessor() {
        var topology = new TopologyBuilder();

        topology.Stream<String>("orders", ValueSerializers.String, StringResolver())
            .Peek((_, _) => ValueTask.CompletedTask);
    }

    [Fact]
    public void StreamCanDeclareExplicitSerializers() {
        var topology = new TopologyBuilder();

        topology.Stream<Int32>("widgets", new Int32Serializer(), new WidgetValueSerializer(), WidgetResolver())
            .Peek((_, _) => ValueTask.CompletedTask);
    }

    [Fact]
    public void StreamCanUseBuiltInKeySerializers() {
        var topology = new TopologyBuilder();

        topology.Stream<Int32>("widgets-by-int", ValueSerializers.String, StringResolver()).Peek((_, _) => ValueTask.CompletedTask);
        topology.Stream<Int64>("widgets-by-long", ValueSerializers.String, StringResolver()).Peek((_, _) => ValueTask.CompletedTask);
        topology.Stream<Guid>("widgets-by-guid", ValueSerializers.String, StringResolver()).Peek((_, _) => ValueTask.CompletedTask);
    }

    [Fact]
    public void TopologyCanMergeStreams() {
        var topology = new TopologyBuilder();
        var orders = topology.Stream<String>("orders", ValueSerializers.String, StringResolver());
        var payments = topology.Stream<String>("payments", ValueSerializers.String, StringResolver());

        topology.Merge(orders, payments)
            .Peek((_, _) => ValueTask.CompletedTask);
    }

    [Fact]
    public void MergeRequiresStreamsFromSameTopology() {
        var first = new TopologyBuilder();
        var second = new TopologyBuilder();

        var error = Assert.Throws<InvalidOperationException>(() => first.Merge(
            first.Stream<String>("orders", ValueSerializers.String, StringResolver()),
            second.Stream<String>("payments", ValueSerializers.String, StringResolver())
        ));

        Assert.Contains("same topology", error.Message);
    }

    [Fact]
    public async Task RunRequiresApplicationId() {
        var options = new StreamlinrOptions { BootstrapServers = "localhost:9092" };

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => StreamlinrApplication.RunAsync(
            options,
            topology => topology.Stream<String>("orders", ValueSerializers.String, StringResolver()).Peek((_, _) => ValueTask.CompletedTask),
            TestContext.Current.CancellationToken
        ));

        Assert.Contains("ApplicationId", error.Message);
    }

    [Fact]
    public async Task RunRequiresBootstrapServers() {
        var options = new StreamlinrOptions { ApplicationId = "orders-app" };

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => StreamlinrApplication.RunAsync(
            options,
            topology => topology.Stream<String>("orders", ValueSerializers.String, StringResolver()).Peek((_, _) => ValueTask.CompletedTask),
            TestContext.Current.CancellationToken
        ));

        Assert.Contains("BootstrapServers", error.Message);
    }

    [Fact]
    public void StreamRequiresExplicitSerializerWhenNoDefaultExists() {
        var topology = new TopologyBuilder();

        var error = Assert.Throws<InvalidOperationException>(() => topology.Stream<Decimal>("orders", ValueSerializers.String, StringResolver()));

        Assert.Contains("default key serializer", error.Message);
    }

    [Fact]
    public async Task InitialRuntimeRequiresProcessor() {
        var options = new StreamlinrOptions { ApplicationId = "orders-app", BootstrapServers = "localhost:9092" };

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => StreamlinrApplication.RunAsync(
            options,
            topology => topology.Stream<String>("orders", ValueSerializers.String, StringResolver()),
            TestContext.Current.CancellationToken
        ));

        Assert.Contains("processor", error.Message);
    }

    sealed record Widget(String Id, String Status);

    static DefaultMessageTypeResolver StringResolver() => new("message-type") {
        ["string"] = typeof(String),
    };

    static DefaultMessageTypeResolver WidgetResolver() => new("message-type") {
        ["widget"] = typeof(Widget),
    };

    sealed class Int32Serializer : IKeySerializer<Int32> {
        public Byte[]? Serialize(Int32 value, SerializationContext context) => BitConverter.GetBytes(value);

        public Int32 Deserialize(Byte[]? data, SerializationContext context) => BitConverter.ToInt32(data ?? throw new ArgumentNullException(nameof(data)));
    }

    sealed class WidgetValueSerializer : IValueSerializer {
        public Byte[] Serialize(Object value, Type valueType, SerializationContext context) => [];

        public Object Deserialize(Byte[] data, Type valueType, SerializationContext context) => new Widget("widget-1", "created");
    }
}
