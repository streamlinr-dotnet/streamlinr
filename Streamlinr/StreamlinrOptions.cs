namespace Streamlinr;

/// <summary>
/// Configures the initial Streamlinr runtime.
/// </summary>
public sealed class StreamlinrOptions {
    /// <summary>
    /// Gets or sets the application identity used as the Kafka consumer group for the initial runtime slice.
    /// </summary>
    public String ApplicationId { get; set; } = String.Empty;

    /// <summary>
    /// Gets or sets the Kafka bootstrap server list.
    /// </summary>
    public String BootstrapServers { get; set; } = String.Empty;
}
