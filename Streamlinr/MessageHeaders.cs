namespace Streamlinr;

/// <summary>
/// Represents Kafka message headers without exposing Kafka client types.
/// Kafka headers are an ordered collection of name/value pairs, not a dictionary:
/// the same name can appear multiple times and insertion order is preserved.
/// </summary>
public sealed class MessageHeaders : ILookup<String, Byte[]> {
    readonly List<(String Name, Byte[] Value)> _headers = [];

    /// <summary>
    /// Initializes an empty header collection.
    /// </summary>
    public MessageHeaders() {
    }

    internal MessageHeaders(IEnumerable<(String Name, Byte[] Value)> headers) {
        _headers.AddRange(headers);
    }

    /// <summary>
    /// Gets all headers in insertion order, including duplicate header names.
    /// </summary>
    public IReadOnlyList<(String Name, Byte[] Value)> All => _headers;

    /// <summary>
    /// Gets the number of headers in the ordered collection, including duplicate names.
    /// </summary>
    public Int32 Count => _headers.Count;

    /// <inheritdoc />
    Int32 ILookup<String, Byte[]>.Count => _headers.Select(header => header.Name).Distinct(StringComparer.Ordinal).Count();

    /// <summary>
    /// Gets all values for a header name in insertion order.
    /// </summary>
    /// <param name="key">The header name.</param>
    /// <returns>All matching header values.</returns>
    public IEnumerable<Byte[]> this[String key] => GetAll(key);

    /// <summary>
    /// Adds a header value. This appends a new header entry and does not replace
    /// existing headers with the same name.
    /// </summary>
    /// <param name="name">The header name.</param>
    /// <param name="value">The header value.</param>
    /// <returns>The current header collection.</returns>
    public MessageHeaders Add(String name, Byte[] value) {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(value);

        _headers.Add((name, value));
        return this;
    }

    /// <summary>
    /// Adds an encoded header value. This appends a new header entry and does not
    /// record the source type; callers are responsible for using compatible
    /// decoders when reading values back.
    /// </summary>
    /// <typeparam name="T">The header value type.</typeparam>
    /// <param name="name">The header name.</param>
    /// <param name="value">The header value.</param>
    /// <param name="encoder">The encoder from the source type to header bytes.</param>
    /// <returns>The current header collection.</returns>
    public MessageHeaders Add<T>(String name, T value, HeaderValueEncoder<T> encoder) {
        ArgumentNullException.ThrowIfNull(encoder);

        return Add(name, encoder(value));
    }

    /// <summary>
    /// Attempts to get the last value for a header name.
    /// </summary>
    /// <param name="name">The header name.</param>
    /// <param name="value">The last matching header value.</param>
    /// <returns>True when a matching header exists; otherwise false.</returns>
    public Boolean TryGetLast(String name, out Byte[] value) {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        for (var index = _headers.Count - 1; index >= 0; index--) {
            var header = _headers[index];

            if (String.Equals(header.Name, name, StringComparison.Ordinal)) {
                value = header.Value;
                return true;
            }
        }

        value = [];
        return false;
    }

    /// <summary>
    /// Attempts to get and decode the last value for a header name. The decoder
    /// is applied to the raw bytes of the last matching header. Streamlinr does not
    /// track the original type or encoding used when the header was added.
    /// </summary>
    /// <typeparam name="T">The decoded header value type.</typeparam>
    /// <param name="name">The header name.</param>
    /// <param name="decoder">The decoder from header bytes to the target type.</param>
    /// <param name="value">The decoded last matching header value.</param>
    /// <returns>True when a matching header exists; otherwise false.</returns>
    public Boolean TryGetLast<T>(String name, HeaderValueDecoder<T> decoder, out T value) {
        ArgumentNullException.ThrowIfNull(decoder);

        if (TryGetLast(name, out var bytes)) {
            value = decoder(bytes);
            return true;
        }

        value = default!;
        return false;
    }

    /// <summary>
    /// Gets all values for a header name in insertion order. If the same name was
    /// added more than once, every matching value is returned.
    /// </summary>
    /// <param name="name">The header name.</param>
    /// <returns>All matching header values.</returns>
    public IReadOnlyList<Byte[]> GetAll(String name) {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        return _headers
            .Where(header => String.Equals(header.Name, name, StringComparison.Ordinal))
            .Select(header => header.Value)
            .ToArray();
    }

    /// <summary>
    /// Gets and decodes all values for a header name in insertion order. The same
    /// decoder is applied to every matching raw byte value. Streamlinr does not
    /// track the original type or encoding used when each header was added, so callers
    /// should only use this when all matching values use the same representation.
    /// </summary>
    /// <typeparam name="T">The decoded header value type.</typeparam>
    /// <param name="name">The header name.</param>
    /// <param name="decoder">The decoder from header bytes to the target type.</param>
    /// <returns>All decoded matching header values.</returns>
    public IReadOnlyList<T> GetAll<T>(String name, HeaderValueDecoder<T> decoder) {
        ArgumentNullException.ThrowIfNull(decoder);

        return GetAll(name)
            .Select(value => decoder(value))
            .ToArray();
    }

    /// <summary>
    /// Determines whether at least one header exists with the specified name.
    /// </summary>
    /// <param name="key">The header name.</param>
    /// <returns>True when a matching header exists; otherwise false.</returns>
    public Boolean Contains(String key) {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);

        return _headers.Any(header => String.Equals(header.Name, key, StringComparison.Ordinal));
    }

    /// <summary>
    /// Enumerates header values grouped by distinct header name. Groups are produced
    /// from the ordered header collection and may contain multiple values.
    /// </summary>
    /// <returns>The grouped header values.</returns>
    public IEnumerator<IGrouping<String, Byte[]>> GetEnumerator() => _headers
        .GroupBy(header => header.Name, header => header.Value, StringComparer.Ordinal)
        .GetEnumerator();

    /// <inheritdoc />
    System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
}

/// <summary>
/// Decodes a header value from bytes to a concrete type.
/// </summary>
/// <typeparam name="T">The decoded header value type.</typeparam>
/// <param name="value">The header value bytes.</param>
/// <returns>The decoded header value.</returns>
public delegate T HeaderValueDecoder<out T>(ReadOnlySpan<Byte> value);

/// <summary>
/// Encodes a concrete value to header bytes.
/// </summary>
/// <typeparam name="T">The encoded header value type.</typeparam>
/// <param name="value">The source header value.</param>
/// <returns>The header value bytes.</returns>
public delegate Byte[] HeaderValueEncoder<in T>(T value);
