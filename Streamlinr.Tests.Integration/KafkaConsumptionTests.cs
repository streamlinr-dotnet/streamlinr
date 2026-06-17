namespace Streamlinr;

using System.Text;
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

        var received = new TaskCompletionSource<StreamRecord<String>>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        using var stopStreamlinr = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token);

        var streamlinrTask = StreamlinrApplication.RunAsync(
            options: new StreamlinrOptions {
                ApplicationId    = $"streamlinr-smoke-{Guid.NewGuid():N}",
                BootstrapServers = _kafka.BootstrapServers,
            },
            configureTopology: topology => {
                topology
                    .Stream<String>(topic, ValueSerializers.String, StringResolver())
                    .Peek((record, _) => {
                        if (received.TrySetResult(record))
                            stopStreamlinr.Cancel();

                        return ValueTask.CompletedTask;
                    });
            },
            cancellationToken: stopStreamlinr.Token);

        await ProduceStringAsync(topic, "order-1", "created", TestContext.Current.CancellationToken);

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
    public async Task MergeReceivesRecordsFromBothSourceTopics() {
        var topicA = $"streamlinr-merge-a-{Guid.NewGuid():N}";
        var topicB = $"streamlinr-merge-b-{Guid.NewGuid():N}";
        await _kafka.CreateTopicAsync(topicA, TestContext.Current.CancellationToken);
        await _kafka.CreateTopicAsync(topicB, TestContext.Current.CancellationToken);

        var received = new List<StreamRecord<String>>();
        var receivedBoth = new TaskCompletionSource<IReadOnlyList<StreamRecord<String>>>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        using var stopStreamlinr = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token);

        var streamlinrTask = StreamlinrApplication.RunAsync(
            options: new StreamlinrOptions {
                ApplicationId    = $"streamlinr-merge-{Guid.NewGuid():N}",
                BootstrapServers = _kafka.BootstrapServers,
            },
            configureTopology: topology => {
                var sourceA = topology.Stream<String>(topicA, ValueSerializers.String, StringResolver());
                var sourceB = topology.Stream<String>(topicB, ValueSerializers.String, StringResolver());

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

        await ProduceStringAsync(topicA, "message-1", "value-1", TestContext.Current.CancellationToken);
        await ProduceStringAsync(topicB, "message-2", "value-2", TestContext.Current.CancellationToken);

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
        Assert.Contains(records, record => record.Key == "message-1" && record.Value is StreamValue.Resolved { Value: "value-1", Type: not null });
        Assert.Contains(records, record => record.Key == "message-2" && record.Value is StreamValue.Resolved { Value: "value-2", Type: not null });
    }

    [Fact]
    public async Task PeekReceivesDeserializedWidget() {
        var topic = $"streamlinr-widget-{Guid.NewGuid():N}";
        await _kafka.CreateTopicAsync(topic, TestContext.Current.CancellationToken);

        var received = new TaskCompletionSource<StreamRecord<String>>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        using var stopStreamlinr = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token);

        var streamlinrTask = StreamlinrApplication.RunAsync(
            options: new StreamlinrOptions {
                ApplicationId    = $"streamlinr-widget-{Guid.NewGuid():N}",
                BootstrapServers = _kafka.BootstrapServers,
            },
            configureTopology: topology => {
                topology
                    .Stream<String>(topic, valueSerializer: new WidgetValueSerializer(), messageTypeResolver: WidgetResolver())
                    .Peek((record, _) => {
                        if (received.TrySetResult(record))
                            stopStreamlinr.Cancel();

                        return ValueTask.CompletedTask;
                    });
            },
            cancellationToken: stopStreamlinr.Token);

        var headers = new MessageHeaders();
        headers.Add("message-type", Encoding.UTF8.GetBytes("widget"));

        await _kafka.ProduceBytesAsync(
            topic,
            (Encoding.UTF8.GetBytes("widget-1"), Encoding.UTF8.GetBytes("widget-1:created")),
            headers,
            TestContext.Current.CancellationToken);

        var timeoutTask = Task.Delay(Timeout.InfiniteTimeSpan, timeout.Token);
        var completedTask = await Task.WhenAny(received.Task, streamlinrTask, timeoutTask);

        if (completedTask == streamlinrTask) {
            await streamlinrTask;
            Assert.Fail("Streamlinr runtime stopped before receiving the expected widget record.");
        }

        Assert.Same(received.Task, completedTask);

        await stopStreamlinr.CancelAsync();
        await streamlinrTask;

        var record = await received.Task;
        Assert.Equal("widget-1", record.Key);
        var value = Assert.IsType<StreamValue.Resolved>(record.Value);
        Assert.Equal(typeof(Widget), value.Type);
        Assert.Equal(new Widget("widget-1", "created"), value.Value);
    }

    [Fact]
    public async Task UnknownMessageTypeFlowsAsUnresolvedValue() {
        var topic = $"streamlinr-unknown-{Guid.NewGuid():N}";
        await _kafka.CreateTopicAsync(topic, TestContext.Current.CancellationToken);

        var received = new TaskCompletionSource<StreamRecord<String>>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        using var stopStreamlinr = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token);

        var streamlinrTask = StreamlinrApplication.RunAsync(
            options: new StreamlinrOptions {
                ApplicationId    = $"streamlinr-unknown-{Guid.NewGuid():N}",
                BootstrapServers = _kafka.BootstrapServers,
            },
            configureTopology: topology => {
                topology
                    .Stream<String>(topic, new WidgetValueSerializer(), WidgetResolver())
                    .Peek((record, _) => {
                        if (received.TrySetResult(record))
                            stopStreamlinr.Cancel();

                        return ValueTask.CompletedTask;
                    });
            },
            cancellationToken: stopStreamlinr.Token);

        var headers = new MessageHeaders().Add("message-type", Encoding.UTF8.GetBytes("unknown"));
        await _kafka.ProduceBytesAsync(topic, (Encoding.UTF8.GetBytes("widget-1"), Encoding.UTF8.GetBytes("widget-1:created")), headers, TestContext.Current.CancellationToken);

        var timeoutTask = Task.Delay(Timeout.InfiniteTimeSpan, timeout.Token);
        var completedTask = await Task.WhenAny(received.Task, streamlinrTask, timeoutTask);

        if (completedTask == streamlinrTask) {
            await streamlinrTask;
            Assert.Fail("Streamlinr runtime stopped before receiving the unresolved record.");
        }

        Assert.Same(received.Task, completedTask);

        await stopStreamlinr.CancelAsync();
        await streamlinrTask;

        var record = await received.Task;
        Assert.Equal("widget-1", record.Key);
        var value = Assert.IsType<StreamValue.Unresolved>(record.Value);
        Assert.Contains("not mapped", value.Reason);
    }

    [Fact]
    public async Task NullMessageValueFlowsAsTombstone() {
        var topic = $"streamlinr-tombstone-{Guid.NewGuid():N}";
        await _kafka.CreateTopicAsync(topic, TestContext.Current.CancellationToken);

        var received = new TaskCompletionSource<StreamRecord<String>>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        using var stopStreamlinr = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token);

        var streamlinrTask = StreamlinrApplication.RunAsync(
            options: new StreamlinrOptions {
                ApplicationId    = $"streamlinr-tombstone-{Guid.NewGuid():N}",
                BootstrapServers = _kafka.BootstrapServers,
            },
            configureTopology: topology => {
                topology
                    .Stream<String>(topic, new WidgetValueSerializer(), WidgetResolver())
                    .Peek((record, _) => {
                        if (received.TrySetResult(record))
                            stopStreamlinr.Cancel();

                        return ValueTask.CompletedTask;
                    });
            },
            cancellationToken: stopStreamlinr.Token);

        await _kafka.ProduceBytesAsync(
            topic,
            (Encoding.UTF8.GetBytes("widget-1"), null),
            new MessageHeaders(),
            TestContext.Current.CancellationToken);

        var timeoutTask = Task.Delay(Timeout.InfiniteTimeSpan, timeout.Token);
        var completedTask = await Task.WhenAny(received.Task, streamlinrTask, timeoutTask);

        if (completedTask == streamlinrTask) {
            await streamlinrTask;
            Assert.Fail("Streamlinr runtime stopped before receiving the tombstone record.");
        }

        Assert.Same(received.Task, completedTask);

        await stopStreamlinr.CancelAsync();
        await streamlinrTask;

        var record = await received.Task;
        Assert.Equal("widget-1", record.Key);
        Assert.IsType<StreamValue.Tombstone>(record.Value);
    }

    [Fact]
    public async Task BadPayloadFlowsAsDeserializationFailedValue() {
        var topic = $"streamlinr-bad-payload-{Guid.NewGuid():N}";
        await _kafka.CreateTopicAsync(topic, TestContext.Current.CancellationToken);

        var received = new TaskCompletionSource<StreamRecord<String>>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        using var stopStreamlinr = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token);

        var streamlinrTask = StreamlinrApplication.RunAsync(
            options: new StreamlinrOptions {
                ApplicationId    = $"streamlinr-bad-payload-{Guid.NewGuid():N}",
                BootstrapServers = _kafka.BootstrapServers,
            },
            configureTopology: topology => {
                topology
                    .Stream<String>(topic, new WidgetValueSerializer(), WidgetResolver())
                    .Peek((record, _) => {
                        if (received.TrySetResult(record))
                            stopStreamlinr.Cancel();

                        return ValueTask.CompletedTask;
                    });
            },
            cancellationToken: stopStreamlinr.Token);

        var headers = new MessageHeaders().Add("message-type", Encoding.UTF8.GetBytes("widget"));
        await _kafka.ProduceBytesAsync(topic, (Encoding.UTF8.GetBytes("widget-1"), Encoding.UTF8.GetBytes("not-a-widget")), headers, TestContext.Current.CancellationToken);

        var timeoutTask = Task.Delay(Timeout.InfiniteTimeSpan, timeout.Token);
        var completedTask = await Task.WhenAny(received.Task, streamlinrTask, timeoutTask);

        if (completedTask == streamlinrTask) {
            await streamlinrTask;
            Assert.Fail("Streamlinr runtime stopped before receiving the deserialization failure record.");
        }

        Assert.Same(received.Task, completedTask);

        await stopStreamlinr.CancelAsync();
        await streamlinrTask;

        var record = await received.Task;
        Assert.Equal("widget-1", record.Key);
        var value = Assert.IsType<StreamValue.DeserializationFailed>(record.Value);
        Assert.Equal(typeof(Widget), value.Type);
        Assert.IsType<InvalidOperationException>(value.Error);
    }

    sealed record Widget(String Id, String Status);

    static DefaultMessageTypeResolver StringResolver() => new("message-type") {
        ["string"] = typeof(String),
    };

    static DefaultMessageTypeResolver WidgetResolver() => new("message-type") {
        ["widget"] = typeof(Widget),
    };

    Task ProduceStringAsync(String topic, String key, String value, CancellationToken cancellationToken) {
        var headers = new MessageHeaders().Add("message-type", Encoding.UTF8.GetBytes("string"));

        return _kafka.ProduceBytesAsync(
            topic,
            (Encoding.UTF8.GetBytes(key), Encoding.UTF8.GetBytes(value)),
            headers,
            cancellationToken);
    }

    sealed class WidgetValueSerializer : IValueSerializer {
        public Byte[] Serialize(Object value, Type valueType, SerializationContext context) {
            var widget = Assert.IsType<Widget>(value);
            context.Headers.Add("message-type", Encoding.UTF8.GetBytes("widget"));
            return Encoding.UTF8.GetBytes($"{widget.Id}:{widget.Status}");
        }

        public Object Deserialize(Byte[] data, Type valueType, SerializationContext context) {
            Assert.Equal(typeof(Widget), valueType);
            Assert.True(context.Headers.TryGetLast("message-type", out var messageType));
            Assert.Equal("widget", Encoding.UTF8.GetString(messageType));

            var parts = Encoding.UTF8.GetString(data).Split(':');
            if (parts.Length != 2)
                throw new InvalidOperationException("Widget payload must contain id and status.");

            return new Widget(parts[0], parts[1]);
        }
    }
}
