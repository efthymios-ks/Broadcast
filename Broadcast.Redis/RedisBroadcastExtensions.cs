using Broadcast.Internal;
using Broadcast.Redis.Internal;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using StackExchange.Redis;

namespace Broadcast.Redis;

public static class RedisBroadcastExtensions
{
    /// <summary>
    /// Redis pub/sub transport, for multi-pod scenarios. Requires an IConnectionMultiplexer in DI.
    /// </summary>
    public static IServiceCollection AddRedisBroadcaster(
        this IServiceCollection services,
        string keyPrefix
    ) => services.AddBroadcaster(serviceProvider
            => new RedisBroadcast(serviceProvider.GetRequiredService<IConnectionMultiplexer>(), keyPrefix));

    /// <summary>
    /// Redis pub/sub broadcaster resolved only under <paramref name="serviceKey"/>, with its own channel prefix.
    /// A library uses it to share a channel across services without touching the app's own broadcaster.
    /// </summary>
    public static IServiceCollection AddKeyedRedisBroadcaster(
        this IServiceCollection services,
        object serviceKey,
        string keyPrefix
    )
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(serviceKey);
        ArgumentException.ThrowIfNullOrWhiteSpace(keyPrefix);

        services.TryAddKeyedSingleton<IBroadcaster>(serviceKey, (serviceProvider, _) => new Broadcaster(new RedisBroadcast(
            multiplexer: serviceProvider.GetRequiredService<IConnectionMultiplexer>(),
            keyPrefix: keyPrefix
        )));

        return services;
    }
}
