using Broadcast.Redis.Tests.Internal;
using StackExchange.Redis;
using Testcontainers.Redis;
using Xunit;

[assembly: AssemblyFixture(typeof(RedisFixture))]

namespace Broadcast.Redis.Tests.Internal;

public sealed class RedisFixture : IAsyncLifetime
{
    private readonly RedisContainer _container = new RedisBuilder("redis:7.4-alpine")
        .Build();

    public IConnectionMultiplexer Multiplexer { get; private set; } = null!;

    public async ValueTask InitializeAsync()
    {
        await _container.StartAsync();
        Multiplexer = await ConnectionMultiplexer.ConnectAsync(_container.GetConnectionString());
    }

    public async ValueTask DisposeAsync()
    {
        if (Multiplexer is not null)
        {
            await Multiplexer.CloseAsync();
            Multiplexer.Dispose();
        }

        await _container.DisposeAsync();
    }
}
