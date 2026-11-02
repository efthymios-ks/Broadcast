using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Broadcast.Internal;

internal sealed class Broadcaster(IBroadcastTransport transport) : IBroadcaster
{
    private static readonly JsonSerializerOptions _jsonOptions = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        NumberHandling = JsonNumberHandling.AllowReadingFromString,
        PropertyNameCaseInsensitive = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false,
    };

    public ValueTask BroadcastAsync<TMessage>(
        string channel,
        TMessage message,
        CancellationToken cancellationToken = default
    ) => transport.BroadcastAsync(
            channel: GetChannelName<TMessage>(channel),
            payload: JsonSerializer.Serialize(message, _jsonOptions),
            cancellationToken: cancellationToken
        );

    public async IAsyncEnumerable<TMessage> Stream<TMessage>(
        string channel,
        [EnumeratorCancellation] CancellationToken cancellationToken
    )
    {
        var channelName = GetChannelName<TMessage>(channel);
        await foreach (var payload in transport.Stream(channelName, cancellationToken))
        {
            if (JsonSerializer.Deserialize<TMessage>(payload, _jsonOptions) is { } message)
            {
                yield return message;
            }
        }
    }

    private static string GetChannelName<TMessage>(string channel)
        => $"{typeof(TMessage).FullName}:{channel}";
}
