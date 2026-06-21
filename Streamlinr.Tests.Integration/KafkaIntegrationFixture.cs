namespace Streamlinr;

using Xunit;

using static System.Text.Encoding;

// ReSharper disable once ClassNeverInstantiated.Global
public sealed class KafkaIntegrationFixture : IAsyncLifetime {
    public KafkaTestContainer Kafka { get; } = new KafkaTestContainer();

    public async ValueTask InitializeAsync() {
        await Kafka.StartAsync(TimeSpan.FromSeconds(60));
    }

    public async ValueTask DisposeAsync() => await Kafka.DisposeAsync();

    static public DefaultMessageTypeResolver StringResolver() => new DefaultMessageTypeResolver {
        ["string"] = typeof(String),
    };

    static public DefaultMessageTypeResolver WidgetResolver() => new DefaultMessageTypeResolver {
        ["widget"] = typeof(Widget),
    };

    public Task ProduceStringAsync(String topic, String key, String value, CancellationToken cancellationToken) {
        var headers = new MessageHeaders {
            { "message-type", "string"u8.ToArray() },
        };

        return Kafka.ProduceBytesAsync(topic, (UTF8.GetBytes(key), UTF8.GetBytes(value)), headers, cancellationToken);
    }

    public sealed record Widget(String Id, String Status);

    public sealed class WidgetValueSerializer : IValueSerializer {
        public Byte[] Serialize(Object value, Type valueType, SerializationContext context) {
            var widget = Assert.IsType<Widget>(value);
            context.Headers.Add("message-type", "widget"u8.ToArray());
            return UTF8.GetBytes($"{widget.Id}:{widget.Status}");
        }

        public Object Deserialize(Byte[] data, Type valueType, SerializationContext context) {
            Assert.Equal(typeof(Widget), valueType);
            Assert.True(context.Headers.TryGetLast("message-type", out var messageType));
            Assert.Equal("widget", UTF8.GetString(messageType));

            var parts = UTF8.GetString(data).Split(':');
            if (parts.Length != 2)
                throw new InvalidOperationException("Widget payload must contain id and status.");

            return new Widget(parts[0], parts[1]);
        }
    }
}
