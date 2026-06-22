namespace Streamlinr;

using System.Buffers.Binary;
using System.Text;
using Xunit;

public sealed class SerializerTests {
    [Fact]
    public void StringSerializerRoundTripsUtf8() {
        var context = new SerializationContext("widgets", new MessageHeaders());

        var data = KeySerializers.String.Serialize("created", context);
        var value = KeySerializers.String.Deserialize(data, context);

        Assert.Equal("created", value);
    }

    [Fact]
    public void BytesSerializerRoundTripsUnchanged() {
        var context = new SerializationContext("widgets", new MessageHeaders());
        var data = new Byte[] { 1, 2, 3 };

        var serialized = KeySerializers.Bytes.Serialize(data, context);
        var deserialized = KeySerializers.Bytes.Deserialize(serialized, context);

        Assert.Same(data, serialized);
        Assert.Same(data, deserialized);
    }

    [Fact]
    public void Int32SerializerUsesBigEndianByteOrder() {
        var context = new SerializationContext("widgets", new MessageHeaders());
        var expected = new Byte[sizeof(Int32)];
        BinaryPrimitives.WriteInt32BigEndian(expected, 123456);

        var data = KeySerializers.Int32.Serialize(123456, context);
        var value = KeySerializers.Int32.Deserialize(data, context);

        Assert.Equal(expected, data);
        Assert.Equal(123456, value);
    }

    [Fact]
    public void Int64SerializerUsesBigEndianByteOrder() {
        var context = new SerializationContext("widgets", new MessageHeaders());
        var expected = new Byte[sizeof(Int64)];
        BinaryPrimitives.WriteInt64BigEndian(expected, 123456789L);

        var data = KeySerializers.Int64.Serialize(123456789L, context);
        var value = KeySerializers.Int64.Deserialize(data, context);

        Assert.Equal(expected, data);
        Assert.Equal(123456789L, value);
    }

    [Fact]
    public void GuidSerializerUsesNetworkByteOrder() {
        var context = new SerializationContext("widgets", new MessageHeaders());
        var guid = Guid.Parse("00112233-4455-6677-8899-aabbccddeeff");
        var expected = new Byte[] {
            0x00, 0x11, 0x22, 0x33,
            0x44, 0x55,
            0x66, 0x77,
            0x88, 0x99,
            0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff,
        };

        var data = KeySerializers.Guid.Serialize(guid, context);
        var value = KeySerializers.Guid.Deserialize(data, context);

        Assert.Equal(expected, data);
        Assert.Equal(guid, value);
    }

    [Theory]
    [InlineData(null)]
    [InlineData(new Byte[] { 1, 2, 3 })]
    [InlineData(new Byte[] { 1, 2, 3, 4, 5 })]
    public void Int32SerializerRejectsInvalidLengths(Byte[]? data) {
        var context = new SerializationContext("widgets", new MessageHeaders());

        Assert.Throws<InvalidOperationException>(() => KeySerializers.Int32.Deserialize(data, context));
    }

    [Theory]
    [InlineData(null)]
    [InlineData(new Byte[] { 1, 2, 3, 4, 5, 6, 7 })]
    [InlineData(new Byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 })]
    public void Int64SerializerRejectsInvalidLengths(Byte[]? data) {
        var context = new SerializationContext("widgets", new MessageHeaders());

        Assert.Throws<InvalidOperationException>(() => KeySerializers.Int64.Deserialize(data, context));
    }

    [Theory]
    [InlineData(null)]
    [InlineData(new Byte[] { 1, 2, 3 })]
    [InlineData(new Byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 })]
    public void GuidSerializerRejectsInvalidLengths(Byte[]? data) {
        var context = new SerializationContext("widgets", new MessageHeaders());

        Assert.Throws<InvalidOperationException>(() => KeySerializers.Guid.Deserialize(data, context));
    }

    [Fact]
    public void CustomWidgetValueSerializerCanUseHeaders() {
        var headers = new MessageHeaders();
        var context = new SerializationContext("widgets", headers);
        var serializer = new WidgetValueSerializer();

        var data = serializer.Serialize(new Widget("widget-1", "created"), typeof(Widget), context);
        var widget = serializer.Deserialize(data, typeof(Widget), context);

        Assert.True(headers.TryGetLast("message-type", out var messageType));
        Assert.Equal("widget", Encoding.UTF8.GetString(messageType));
        Assert.Equal(new Widget("widget-1", "created"), widget);
    }

    [Fact]
    public void DefaultMessageTypeResolverMapsHeaderToType() {
        var resolver = new DefaultMessageTypeResolver("message-type") {
            ["widget-v1"] = typeof(Widget),
        };
        var headers = new MessageHeaders().Add("message-type", Encoding.UTF8.GetBytes("widget-v1"));
        var context = new SerializationContext("widgets", headers);

        var resolution = resolver.ResolveType(context);

        var resolved = Assert.IsType<MessageTypeResolution.Resolved>(resolution);
        Assert.Equal(typeof(Widget), resolved.Type);
    }

    [Fact]
    public void DefaultMessageTypeResolverReturnsUnresolvedForMissingOrUnknownHeader() {
        var resolver = new DefaultMessageTypeResolver("message-type") {
            ["widget-v1"] = typeof(Widget),
        };

        var missing = resolver.ResolveType(new SerializationContext("widgets", new MessageHeaders()));
        var unknown = resolver.ResolveType(new SerializationContext("widgets", new MessageHeaders().Add("message-type", Encoding.UTF8.GetBytes("unknown"))));

        Assert.Contains("not present", Assert.IsType<MessageTypeResolution.Unresolved>(missing).Reason);
        Assert.Contains("not mapped", Assert.IsType<MessageTypeResolution.Unresolved>(unknown).Reason);
    }

    [Fact]
    public void DefaultMessageTypeResolverWritesMappedTypeHeader() {
        var resolver = new DefaultMessageTypeResolver("message-type") {
            ["widget-v1"] = typeof(Widget),
        };
        var headers = new MessageHeaders();
        var context = new SerializationContext("widgets", headers);

        resolver.WriteType(typeof(Widget), context);

        Assert.True(headers.TryGetLast<String>("message-type", Encoding.UTF8.GetString, out var messageType));
        Assert.Equal("widget-v1", messageType);
    }

    [Fact]
    public void DefaultMessageTypeResolverRejectsDuplicateMappings() {
        var duplicateMessageType = new DefaultMessageTypeResolver("message-type") {
            ["widget-v1"] = typeof(Widget),
        };
        var duplicateClrType = new DefaultMessageTypeResolver("message-type") {
            ["widget-v1"] = typeof(Widget),
        };

        Assert.Throws<InvalidOperationException>(() => duplicateMessageType["widget-v1"] = typeof(OtherWidget));
        Assert.Throws<InvalidOperationException>(() => duplicateClrType["widget-v2"] = typeof(Widget));
    }

    [Fact]
    public void MessageHeadersImplementLookup() {
        var headers = new MessageHeaders();
        headers.Add("message-type", Encoding.UTF8.GetBytes("widget"));
        headers.Add("message-type", Encoding.UTF8.GetBytes("widget-v2"));
        headers.Add("correlation-id", Encoding.UTF8.GetBytes("abc"));

        var messageTypes = headers["message-type"].Select(Encoding.UTF8.GetString).ToArray();

        Assert.Equal(3, headers.Count);
        Assert.Equal(2, ((ILookup<String, Byte[]>)headers).Count);
        Assert.True(headers.Contains("message-type"));
        Assert.False(headers.Contains("missing"));
        Assert.Equal(["widget", "widget-v2"], messageTypes);
        Assert.Empty(headers["missing"]);
        Assert.Equal(["correlation-id", "message-type"], headers.Select(group => group.Key).Order().ToArray());
    }

    [Fact]
    public void MessageHeadersAddSupportsFluentChaining() {
        var headers = new MessageHeaders()
            .Add("message-type", Encoding.UTF8.GetBytes("widget"))
            .Add<Int32>("attempt", 1, BitConverter.GetBytes);

        Assert.Equal(2, headers.Count);
        Assert.True(headers.Contains("message-type"));
        Assert.True(headers.Contains("attempt"));
    }

    [Fact]
    public void MessageHeadersConvertValues() {
        var headers = new MessageHeaders();
        headers.Add<Int32>("attempt", 1, BitConverter.GetBytes);
        headers.Add<Int32>("attempt", 2, BitConverter.GetBytes);
        headers.Add<Int32>("big-endian-attempt", 3, value => {
            var bytes = new Byte[sizeof(Int32)];
            BinaryPrimitives.WriteInt32BigEndian(bytes, value);
            return bytes;
        });

        var attempts = headers.GetAll<Int32>("attempt", BitConverter.ToInt32);
        var found = headers.TryGetLast<Int32>("attempt", BitConverter.ToInt32, out var lastAttempt);
        var missing = headers.TryGetLast<Int32>("missing", BitConverter.ToInt32, out var missingAttempt);
        var bigEndianAttempts = headers.GetAll<Int32>("big-endian-attempt", BinaryPrimitives.ReadInt32BigEndian);

        Assert.Equal([1, 2], attempts);
        Assert.True(found);
        Assert.Equal(2, lastAttempt);
        Assert.False(missing);
        Assert.Equal(0, missingAttempt);
        Assert.Equal([3], bigEndianAttempts);
    }

    [Fact]
    public void ValueFailureContinueAsDeadLetterAppliesToAllValueFailures() {
        var failure = ValueFailure.ContinueAsDeadLetter();

        Assert.Equal(ValueFailureAction.ContinueAsDeadLetter(), failure.Unresolved);
        Assert.Equal(ValueFailureAction.ContinueAsDeadLetter(), failure.DeserializationFailed);
    }

    [Fact]
    public void ValueFailureCanSpecifyPhaseSpecificActions() {
        var unresolved = ValueFailureAction.Skip();
        var deserializationFailed = ValueFailureAction.PausePartition();

        var failure = ValueFailure.On(unresolved, deserializationFailed);

        Assert.Same(unresolved, failure.Unresolved);
        Assert.Same(deserializationFailed, failure.DeserializationFailed);
    }

    [Fact]
    public void ValueFailureRequiresBothPhaseActions() {
        Assert.Throws<ArgumentNullException>(() => ValueFailure.On(null!, ValueFailureAction.ContinueAsDeadLetter()));
        Assert.Throws<ArgumentNullException>(() => ValueFailure.On(ValueFailureAction.ContinueAsDeadLetter(), null!));
    }

    [Fact]
    public void ValueFailureActionsAreDistinct() {
        Assert.NotEqual(ValueFailureAction.Skip(), ValueFailureAction.ContinueAsDeadLetter());
        Assert.NotEqual(ValueFailureAction.Skip(), ValueFailureAction.PausePartition());
        Assert.NotEqual(ValueFailureAction.ContinueAsDeadLetter(), ValueFailureAction.PausePartition());
    }

    [Fact]
    public void DeadLetterHandlingActionsAreDistinct() {
        Assert.NotEqual(DeadLetterHandling.Fail(), DeadLetterHandling.Skip());
    }

    sealed record Widget(String Id, String Status);
    sealed record OtherWidget(String Id, String Status);

    sealed class WidgetValueSerializer : IValueSerializer {
        public Byte[] Serialize(Object value, Type valueType, SerializationContext context) {
            var widget = Assert.IsType<Widget>(value);
            context.Headers.Add("message-type", Encoding.UTF8.GetBytes("widget"));
            return Encoding.UTF8.GetBytes($"{widget.Id}:{widget.Status}");
        }

        public Object Deserialize(Byte[] data, Type valueType, SerializationContext context) {
            Assert.Equal(typeof(Widget), valueType);
            var parts = Encoding.UTF8.GetString(data).Split(':');
            return new Widget(parts[0], parts[1]);
        }
    }
}
