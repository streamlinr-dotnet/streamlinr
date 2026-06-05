namespace Streamlinr;

using Confluent.Kafka;
using Confluent.Kafka.Admin;
using DotNet.Testcontainers.Builders;
using Testcontainers.Kafka;

class KafkaTestContainer : IAsyncDisposable {
    readonly KafkaContainer _container = new KafkaBuilder()
        .WithImage("confluentinc/cp-kafka:7.9.1")
        .WithWaitStrategy(Wait.ForUnixContainer().UntilInternalTcpPortIsAvailable(9092))
        .Build();

    public String BootstrapServers => _container.GetBootstrapAddress();

    public async Task StartAsync(TimeSpan timeout) {
        using var timeoutToken = new CancellationTokenSource(timeout);
        await _container.StartAsync(timeoutToken.Token);
    }

    public async Task CreateTopicAsync(String topic, CancellationToken cancellationToken) {
        using var admin = new AdminClientBuilder(new AdminClientConfig {
            BootstrapServers = BootstrapServers,
        }).Build();

        await admin.CreateTopicsAsync(
            topics : [new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 1 }],
            options: new CreateTopicsOptions {
                OperationTimeout = TimeSpan.FromSeconds(10),
                RequestTimeout   = TimeSpan.FromSeconds(10),
            });
    }

    public Task ProduceAsync(String topic, (String Key, String Value) message, CancellationToken cancellationToken) =>
        ProduceBatchAsync(topic, [message], cancellationToken);

    public async Task ProduceBatchAsync(String topic, IReadOnlyCollection<(String Key, String Value)> messages, CancellationToken cancellationToken) {
        ArgumentException.ThrowIfNullOrEmpty(topic);
        ArgumentNullException.ThrowIfNull(messages);

        if (messages.Count == 0) {
            throw new ArgumentException("At least one message is required.", nameof(messages));
        }

        using var producer = new ProducerBuilder<String, String>(new ProducerConfig {
            BootstrapServers = BootstrapServers,
            MessageTimeoutMs = 10_000,
        }).Build();

        foreach (var message in messages) {
            await producer.ProduceAsync(topic, new Message<String, String> { Key = message.Key, Value = message.Value }, cancellationToken);
        }

        producer.Flush(TimeSpan.FromSeconds(10));
    }

    public async ValueTask DisposeAsync() {
        await _container.DisposeAsync();
    }
}
