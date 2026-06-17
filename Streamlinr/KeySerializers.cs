namespace Streamlinr;

using System.Buffers.Binary;
using System.Text;

/// <summary>
/// Built-in serializers for common Kafka message key types.
/// </summary>
static public class KeySerializers {
    /// <summary>
    /// Gets a UTF-8 string key serializer.
    /// </summary>
    static public IKeySerializer<String> String { get; } = new StringSerializer();

    /// <summary>
    /// Gets a byte array key serializer.
    /// </summary>
    static public IKeySerializer<Byte[]> Bytes { get; } = new BytesSerializer();

    /// <summary>
    /// Gets a 32-bit signed integer key serializer using big-endian byte order.
    /// </summary>
    static public IKeySerializer<Int32> Int32 { get; } = new Int32Serializer();

    /// <summary>
    /// Gets a 64-bit signed integer key serializer using big-endian byte order.
    /// </summary>
    static public IKeySerializer<Int64> Int64 { get; } = new Int64Serializer();

    /// <summary>
    /// Gets a GUID key serializer using RFC 4122/network byte order.
    /// </summary>
    static public IKeySerializer<Guid> Guid { get; } = new GuidSerializer();

    sealed class StringSerializer : IKeySerializer<String> {
        public Byte[]? Serialize(String? value, SerializationContext context) =>
            value is null ? null : Encoding.UTF8.GetBytes(value);

        public String? Deserialize(Byte[]? data, SerializationContext context) =>
            data is null ? null : Encoding.UTF8.GetString(data);
    }

    sealed class BytesSerializer : IKeySerializer<Byte[]> {
        public Byte[]? Serialize(Byte[]? value, SerializationContext context) => value;

        public Byte[]? Deserialize(Byte[]? data, SerializationContext context) => data;
    }

    sealed class Int32Serializer : IKeySerializer<Int32> {
        public Byte[] Serialize(Int32 value, SerializationContext context) {
            var data = new Byte[sizeof(Int32)];
            BinaryPrimitives.WriteInt32BigEndian(data, value);
            return data;
        }

        public Int32 Deserialize(Byte[]? data, SerializationContext context) {
            EnsureLength(data, sizeof(Int32), nameof(Int32));
            return BinaryPrimitives.ReadInt32BigEndian(data);
        }
    }

    sealed class Int64Serializer : IKeySerializer<Int64> {
        public Byte[] Serialize(Int64 value, SerializationContext context) {
            var data = new Byte[sizeof(Int64)];
            BinaryPrimitives.WriteInt64BigEndian(data, value);
            return data;
        }

        public Int64 Deserialize(Byte[]? data, SerializationContext context) {
            EnsureLength(data, sizeof(Int64), nameof(Int64));
            return BinaryPrimitives.ReadInt64BigEndian(data);
        }
    }

    sealed class GuidSerializer : IKeySerializer<Guid> {
        public Byte[] Serialize(Guid value, SerializationContext context) {
            var data = new Byte[16];
            value.TryWriteBytes(data, bigEndian: true, out _);
            return data;
        }

        public Guid Deserialize(Byte[]? data, SerializationContext context) {
            EnsureLength(data, 16, nameof(Guid));
            return new Guid(data, bigEndian: true);
        }
    }

    static void EnsureLength(Byte[]? data, Int32 expectedLength, String typeName) {
        if (data is null)
            throw new InvalidOperationException($"Cannot deserialize a null Kafka key as {typeName}.");

        if (data.Length != expectedLength)
            throw new InvalidOperationException($"Cannot deserialize {data.Length} bytes as {typeName}; expected {expectedLength} bytes.");
    }
}
