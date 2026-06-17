namespace Streamlinr;

using System.Text;

/// <summary>
/// Resolves message value types using a UTF-8 message type header and a bidirectional type map.
/// </summary>
public sealed class DefaultMessageTypeResolver : IMessageTypeResolver {
    const String DefaultHeaderName = "message-type";

    readonly Dictionary<String, Type> _typesByName = new(StringComparer.Ordinal);
    readonly Dictionary<Type, String> _namesByType = [];

    /// <summary>
    /// Initializes a resolver that reads and writes message type names using the specified header.
    /// </summary>
    /// <param name="headerName">The message type header name.</param>
    public DefaultMessageTypeResolver(String headerName = DefaultHeaderName) {
        ArgumentException.ThrowIfNullOrWhiteSpace(headerName);

        HeaderName = headerName;
    }

    /// <summary>
    /// Gets the message type header name.
    /// </summary>
    public String HeaderName { get; }

    /// <summary>
    /// Gets or sets a message type mapping.
    /// </summary>
    /// <param name="messageType">The message type header value.</param>
    /// <returns>The mapped CLR type.</returns>
    public Type this[String messageType] {
        get => _typesByName[messageType];
        set => Add(messageType, value);
    }

    /// <inheritdoc />
    public MessageTypeResolution ResolveType(SerializationContext context) {
        ArgumentNullException.ThrowIfNull(context);

        if (!context.Headers.TryGetLast<String>(HeaderName, Encoding.UTF8.GetString, out var messageType))
            return new MessageTypeResolution.Unresolved($"Message type header '{HeaderName}' was not present.");

        if (!_typesByName.TryGetValue(messageType, out var type))
            return new MessageTypeResolution.Unresolved($"Message type '{messageType}' is not mapped.");

        return new MessageTypeResolution.Resolved(type);
    }

    /// <inheritdoc />
    public void WriteType(Type valueType, SerializationContext context) {
        ArgumentNullException.ThrowIfNull(valueType);
        ArgumentNullException.ThrowIfNull(context);

        if (!_namesByType.TryGetValue(valueType, out var messageType))
            throw new InvalidOperationException($"CLR type '{valueType.FullName}' is not mapped to a message type.");

        context.Headers.Add(HeaderName, messageType, Encoding.UTF8.GetBytes);
    }

    void Add(String messageType, Type type) {
        ArgumentException.ThrowIfNullOrWhiteSpace(messageType);
        ArgumentNullException.ThrowIfNull(type);

        if (_typesByName.TryGetValue(messageType, out var existingType) && existingType != type)
            throw new InvalidOperationException($"Message type '{messageType}' is already mapped to CLR type '{existingType.FullName}'.");

        if (_namesByType.TryGetValue(type, out var existingMessageType) && !String.Equals(existingMessageType, messageType, StringComparison.Ordinal))
            throw new InvalidOperationException($"CLR type '{type.FullName}' is already mapped to message type '{existingMessageType}'.");

        _typesByName[messageType] = type;
        _namesByType[type] = messageType;
    }
}
