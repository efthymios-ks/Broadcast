using Broadcast.Internal;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Broadcast;

public static class BroadcastExtensions
{
    /// <summary>
    /// In-process pub/sub transport. Only works within a single app instance,
    /// but has no external dependencies and is very fast.
    /// </summary>
    public static IServiceCollection AddBroadcaster(this IServiceCollection services)
        => services.AddBroadcaster<InProcessBroadcast>();

    /// <summary>
    /// Custom transport, with type inferred from DI. The transport must be registered in DI,
    /// or have a public constructor that can be fulfilled by DI.
    /// </summary>
    public static IServiceCollection AddBroadcaster<TTransport>(this IServiceCollection services)
        where TTransport : class, IBroadcastTransport
        => services.AddBroadcaster(ActivatorUtilities.GetServiceOrCreateInstance<TTransport>);

    internal static IServiceCollection AddBroadcaster<TTransport>(
            this IServiceCollection services,
            Func<IServiceProvider, TTransport> factory
    ) where TTransport : class, IBroadcastTransport
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(factory);

        services.TryAddSingleton(TimeProvider.System);
        services.TryAddSingleton<IBroadcaster, Broadcaster>();
        services.TryAddSingleton<IBroadcastTransport>(factory);

        return services;
    }
}
