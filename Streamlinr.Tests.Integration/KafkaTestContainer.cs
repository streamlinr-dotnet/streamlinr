namespace Streamlinr;

using System.Net;
using System.Net.Http.Json;
using System.Net.Sockets;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;

public class KafkaTestContainer : IAsyncDisposable {
    const String KafkaAlias = "kafka";
    const String ToxiproxyAlias = "toxiproxy";
    const String KafkaProxyName = "kafka";
    const Int32 KafkaAppPort = 9092;
    const Int32 KafkaDirectPort = 19092;
    const Int32 KafkaBrokerPort = 9093;
    const Int32 KafkaControllerPort = 9094;
    const Int32 ToxiproxyControlPort = 8474;
    const Int32 ToxiproxyKafkaPort = 8666;

    static readonly KafkaTestContainerConfiguration DefaultConfiguration = new KafkaTestContainerConfiguration(
        Image      : "confluentinc/cp-kafka:7.9.1",
        StartupMode: KafkaStartupMode.ConfluentKRaft
    );

    readonly KafkaTestContainerConfiguration _configuration;
    readonly Int32                           _appHostPort     = GetAvailableTcpPort();
    readonly Int32                           _directHostPort  = GetAvailableTcpPort();
    readonly HttpClient                      _toxiproxyClient = new HttpClient();
    readonly INetwork                        _network;
    readonly IContainer                      _toxiproxy;
    readonly IContainer                      _kafka;

    public KafkaTestContainer()
        : this(DefaultConfiguration) {
    }

    KafkaTestContainer(KafkaTestContainerConfiguration configuration) {
        _configuration = configuration;
        _network = new NetworkBuilder().Build();

        _toxiproxy = new ContainerBuilder()
            .WithImage("ghcr.io/shopify/toxiproxy:2.12.0")
            .WithNetwork(_network)
            .WithNetworkAliases(ToxiproxyAlias)
            .WithPortBinding(ToxiproxyControlPort, true)
            .WithPortBinding(_appHostPort, ToxiproxyKafkaPort)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilHttpRequestIsSucceeded(request => request.ForPath("/version").ForPort(ToxiproxyControlPort)))
            .Build();

        _kafka = new ContainerBuilder()
            .WithImage(_configuration.Image)
            .WithNetwork(_network)
            .WithNetworkAliases(KafkaAlias)
            .WithPortBinding(_directHostPort, KafkaDirectPort)
            .WithEnvironment(BuildKafkaEnvironment(_configuration, BootstrapServers, DirectBootstrapServers))
            .WithWaitStrategy(Wait.ForUnixContainer().UntilInternalTcpPortIsAvailable(KafkaDirectPort))
            .Build();
    }

    public String BootstrapServers => $"localhost:{_appHostPort}";

    String DirectBootstrapServers => $"localhost:{_directHostPort}";

    Uri ToxiproxyBaseAddress => new UriBuilder(Uri.UriSchemeHttp, _toxiproxy.Hostname, _toxiproxy.GetMappedPublicPort(ToxiproxyControlPort)).Uri;

    public async Task StartAsync(TimeSpan timeout) {
        using var timeoutToken = new CancellationTokenSource(timeout);

        await _toxiproxy.StartAsync(timeoutToken.Token);
        _toxiproxyClient.BaseAddress = ToxiproxyBaseAddress;

        await CreateKafkaProxyAsync(timeoutToken.Token);
        await _kafka.StartAsync(timeoutToken.Token);

        await WaitForKafkaAsync(DirectBootstrapServers, () => GetKafkaLogsAsync(timeoutToken.Token), timeoutToken.Token);
        await WaitForKafkaAsync(BootstrapServers, () => GetKafkaLogsAsync(timeoutToken.Token), timeoutToken.Token);
    }

    public Task CreateTopicAsync(String topic, CancellationToken cancellationToken) => CreateTopicAsync(topic, partitionCount: 1, cancellationToken);

    public async Task CreateTopicAsync(String topic, Int32 partitionCount, CancellationToken cancellationToken) {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(partitionCount);

        using var admin = new AdminClientBuilder(new AdminClientConfig {
            BootstrapServers = DirectBootstrapServers,
        }).Build();

        await admin.CreateTopicsAsync(
            topics : [new TopicSpecification { Name = topic, NumPartitions = partitionCount, ReplicationFactor = 1 }],
            options: new CreateTopicsOptions {
                OperationTimeout = TimeSpan.FromSeconds(10),
                RequestTimeout   = TimeSpan.FromSeconds(10),
            });
    }

    public Task ProduceAsync(String topic, (String Key, String Value) message, CancellationToken cancellationToken) =>
        ProduceBatchAsync(topic, [message], cancellationToken);

    public async Task ProduceBytesAsync(String topic, (Byte[] Key, Byte[]? Value) message, MessageHeaders headers, CancellationToken cancellationToken) {
        ArgumentException.ThrowIfNullOrEmpty(topic);
        ArgumentNullException.ThrowIfNull(headers);

        using var producer = new ProducerBuilder<Byte[], Byte[]>(new ProducerConfig {
            BootstrapServers = DirectBootstrapServers,
            MessageTimeoutMs = 10_000,
        }).Build();

        await producer.ProduceAsync(topic, new Message<Byte[], Byte[]> {
            Key     = message.Key,
            Value   = message.Value!,
            Headers = ToKafkaHeaders(headers),
        }, cancellationToken);

        producer.Flush(TimeSpan.FromSeconds(10));
    }

    public async Task ProduceBytesAsync(String topic, Int32 partition, (Byte[] Key, Byte[]? Value) message, MessageHeaders headers, CancellationToken cancellationToken) {
        ArgumentException.ThrowIfNullOrEmpty(topic);
        ArgumentOutOfRangeException.ThrowIfNegative(partition);
        ArgumentNullException.ThrowIfNull(headers);

        using var producer = new ProducerBuilder<Byte[], Byte[]>(new ProducerConfig {
            BootstrapServers = DirectBootstrapServers,
            MessageTimeoutMs = 10_000,
        }).Build();

        await producer.ProduceAsync(new TopicPartition(topic, new Partition(partition)), new Message<Byte[], Byte[]> {
            Key     = message.Key,
            Value   = message.Value!,
            Headers = ToKafkaHeaders(headers),
        }, cancellationToken);

        producer.Flush(TimeSpan.FromSeconds(10));
    }

    public async Task ProduceBatchAsync(String topic, IReadOnlyCollection<(String Key, String Value)> messages, CancellationToken cancellationToken) {
        ArgumentException.ThrowIfNullOrEmpty(topic);
        ArgumentNullException.ThrowIfNull(messages);

        if (messages.Count == 0) {
            throw new ArgumentException("At least one message is required.", nameof(messages));
        }

        using var producer = new ProducerBuilder<String, String>(new ProducerConfig {
            BootstrapServers = DirectBootstrapServers,
            MessageTimeoutMs = 10_000,
        }).Build();

        foreach (var message in messages) {
            await producer.ProduceAsync(topic, new Message<String, String> { Key = message.Key, Value = message.Value }, cancellationToken);
        }

        producer.Flush(TimeSpan.FromSeconds(10));
    }

    public async Task RestoreNetworkAsync(CancellationToken cancellationToken) {
        await ResetToxiproxyAsync(cancellationToken);
        await CreateKafkaProxyAsync(cancellationToken);
    }

    public Task DisconnectKafkaAsync(CancellationToken cancellationToken) =>
        SetKafkaProxyEnabledAsync(enabled: false, cancellationToken);

    public Task ReconnectKafkaAsync(CancellationToken cancellationToken) =>
        SetKafkaProxyEnabledAsync(enabled: true, cancellationToken);

    public async Task AddKafkaLatencyAsync(TimeSpan latency, TimeSpan jitter, CancellationToken cancellationToken) {
        await RestoreNetworkAsync(cancellationToken);
        await AddToxicAsync("latency_downstream", "latency", "downstream", new {
            latency = Convert.ToInt64(latency.TotalMilliseconds),
            jitter = Convert.ToInt64(jitter.TotalMilliseconds),
        }, cancellationToken);
    }

    public async Task LimitKafkaBandwidthAsync(Int32 kilobytesPerSecond, CancellationToken cancellationToken) {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(kilobytesPerSecond);

        await RestoreNetworkAsync(cancellationToken);
        await AddToxicAsync("bandwidth_downstream", "bandwidth", "downstream", new {
            rate = kilobytesPerSecond,
        }, cancellationToken);
    }

    public async Task TimeoutKafkaTrafficAsync(TimeSpan timeout, CancellationToken cancellationToken) {
        await RestoreNetworkAsync(cancellationToken);
        await AddToxicAsync("timeout_downstream", "timeout", "downstream", new {
            timeout = Convert.ToInt64(timeout.TotalMilliseconds),
        }, cancellationToken);
    }

    public async ValueTask DisposeAsync() {
        _toxiproxyClient.Dispose();
        await _kafka.DisposeAsync();
        await _toxiproxy.DisposeAsync();
        await _network.DisposeAsync();
    }

    static IReadOnlyDictionary<String, String> BuildKafkaEnvironment(KafkaTestContainerConfiguration configuration, String appBootstrapServers, String directBootstrapServers) {
        if (configuration.StartupMode != KafkaStartupMode.ConfluentKRaft) {
            throw new NotSupportedException($"Unsupported Kafka startup mode: {configuration.StartupMode}.");
        }

        return new Dictionary<String, String> {
            ["CLUSTER_ID"] = "4L6g3nShT-eMCtK--X86sw",
            ["KAFKA_NODE_ID"] = "1",
            ["KAFKA_PROCESS_ROLES"] = "broker,controller",
            ["KAFKA_CONTROLLER_QUORUM_VOTERS"] = $"1@localhost:{KafkaControllerPort}",
            ["KAFKA_LISTENERS"] = $"APP://0.0.0.0:{KafkaAppPort},DIRECT://0.0.0.0:{KafkaDirectPort},BROKER://0.0.0.0:{KafkaBrokerPort},CONTROLLER://0.0.0.0:{KafkaControllerPort}",
            ["KAFKA_ADVERTISED_LISTENERS"] = $"APP://{appBootstrapServers},DIRECT://{directBootstrapServers},BROKER://{KafkaAlias}:{KafkaBrokerPort}",
            ["KAFKA_LISTENER_SECURITY_PROTOCOL_MAP"] = "APP:PLAINTEXT,DIRECT:PLAINTEXT,BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT",
            ["KAFKA_INTER_BROKER_LISTENER_NAME"] = "BROKER",
            ["KAFKA_CONTROLLER_LISTENER_NAMES"] = "CONTROLLER",
            ["KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR"] = "1",
            ["KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS"] = "1",
            ["KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR"] = "1",
            ["KAFKA_TRANSACTION_STATE_LOG_MIN_ISR"] = "1",
            ["KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS"] = "0",
            ["KAFKA_LOG_FLUSH_INTERVAL_MESSAGES"] = Int64.MaxValue.ToString(),
        };
    }

    async Task<String> GetKafkaLogsAsync(CancellationToken cancellationToken) {
        var (stdout, stderr) = await _kafka.GetLogsAsync(ct: cancellationToken);
        return stdout + Environment.NewLine + stderr;
    }

    static async Task WaitForKafkaAsync(String bootstrapServers, Func<Task<String>> getLogsAsync, CancellationToken cancellationToken) {
        var deadline = DateTimeOffset.UtcNow.AddSeconds(30);

        while (DateTimeOffset.UtcNow < deadline) {
            cancellationToken.ThrowIfCancellationRequested();

            try {
                using var admin = new AdminClientBuilder(new AdminClientConfig {
                    BootstrapServers = bootstrapServers,
                    SocketTimeoutMs  = 1_000,
                }).Build();

                admin.GetMetadata(TimeSpan.FromSeconds(1));
                return;
            }
            catch (KafkaException) {
                await Task.Delay(TimeSpan.FromMilliseconds(250), cancellationToken);
            }
        }

        throw new TimeoutException($"Kafka did not become available at {bootstrapServers}.{Environment.NewLine}{await getLogsAsync()}");
    }

    static Int32 GetAvailableTcpPort() {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }

    static Headers ToKafkaHeaders(MessageHeaders headers) {
        var kafkaHeaders = new Headers();

        foreach (var header in headers.All) {
            kafkaHeaders.Add(header.Name, header.Value);
        }

        return kafkaHeaders;
    }

    async Task CreateKafkaProxyAsync(CancellationToken cancellationToken) {
        var response = await _toxiproxyClient.PostAsJsonAsync("/proxies", new {
            name     = KafkaProxyName,
            listen   = $"0.0.0.0:{ToxiproxyKafkaPort}",
            upstream = $"{KafkaAlias}:{KafkaAppPort}",
            enabled  = true,
        }, cancellationToken);

        if (response.StatusCode == HttpStatusCode.Conflict) {
            return;
        }

        response.EnsureSuccessStatusCode();
    }

    async Task ResetToxiproxyAsync(CancellationToken cancellationToken) {
        var response = await _toxiproxyClient.PostAsync("/reset", content: null, cancellationToken);
        response.EnsureSuccessStatusCode();
    }

    async Task SetKafkaProxyEnabledAsync(Boolean enabled, CancellationToken cancellationToken) {
        var response = await _toxiproxyClient.PostAsJsonAsync($"/proxies/{KafkaProxyName}", new {
            enabled,
        }, cancellationToken);

        response.EnsureSuccessStatusCode();
    }

    async Task AddToxicAsync(String name, String type, String stream, Object attributes, CancellationToken cancellationToken) {
        var response = await _toxiproxyClient.PostAsJsonAsync($"/proxies/{KafkaProxyName}/toxics", new {
            name,
            type,
            stream,
            toxicity = 1,
            attributes,
        }, cancellationToken);

        response.EnsureSuccessStatusCode();
    }

    sealed record KafkaTestContainerConfiguration(String Image, KafkaStartupMode StartupMode);

    enum KafkaStartupMode {
        ConfluentKRaft,
    }
}
