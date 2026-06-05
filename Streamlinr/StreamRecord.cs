namespace Streamlinr;

/// <summary>
/// A record observed by a Streamlinr processor.
/// </summary>
/// <typeparam name="TKey">The record key type.</typeparam>
/// <typeparam name="TValue">The record value type.</typeparam>
public sealed record StreamRecord<TKey, TValue>(TKey Key, TValue Value);
