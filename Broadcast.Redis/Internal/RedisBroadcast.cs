using Broadcast.Internal;
using StackExchange.Redis;

namespace Broadcast.Redis.Internal;

internal sealed class RedisBroadcast(
    IConnectionMultiplexer multiplexer,
    string keyPrefix
    ) : FanOutTransportBase
{
    private readonly IConnectionMultiplexer _multiplexer = multiplexer;
    private readonly string _keyPrefix = keyPrefix.TrimEnd(':');

    public override async ValueTask BroadcastAsync(
        string channel,
        string payload,
        CancellationToken cancellationToken = default
    ) => await _multiplexer
        .GetSubscriber()
        .PublishAsync(GetRedisChannel(channel), payload);

    protected override async ValueTask<IAsyncDisposable?> ConnectUpstreamAsync(
        string channel,
        CancellationToken cancellationToken
    )
    {
        var subscriber = _multiplexer.GetSubscriber();
        var redisChannel = GetRedisChannel(channel);

        await subscriber.SubscribeAsync(redisChannel, (_, value) =>
        {
            if (value.HasValue)
            {
                Publish(channel, value!);
            }
        });

        return new RedisSubscription(subscriber, redisChannel);
    }

    private RedisChannel GetRedisChannel(string channel)
        => RedisChannel.Literal($"{_keyPrefix}:{channel}");

    private sealed class RedisSubscription(ISubscriber subscriber, RedisChannel channel)
        : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
            => await subscriber.UnsubscribeAsync(channel);
    }
}
