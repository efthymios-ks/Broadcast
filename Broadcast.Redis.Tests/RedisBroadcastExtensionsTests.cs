using Broadcast.Internal;
using Broadcast.Redis.Internal;
using Broadcast.Redis.Tests.Internal;
using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;
using Xunit;

namespace Broadcast.Redis.Tests;

public sealed class RedisBroadcastExtensionsTests(RedisFixture fixture)
{
    private static readonly TimeSpan _timeout = TimeSpan.FromSeconds(5);

    [Fact]
    public void AddRedisBroadcaster_WhenCalled_ShouldRegisterRedisTransport()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddSingleton(fixture.Multiplexer);

        // Act
        services.AddRedisBroadcaster("orders:");

        // Assert
        var provider = services.BuildServiceProvider();
        Assert.IsType<RedisBroadcast>(provider.GetRequiredService<IBroadcastTransport>());
    }

    [Fact]
    public void AddKeyedRedisBroadcaster_WhenCalled_ShouldResolveOnlyUnderItsKey()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddSingleton(fixture.Multiplexer);

        // Act
        services.AddKeyedRedisBroadcaster(
            serviceKey: "sitecore",
            keyPrefix: "sitecore"
        );

        // Assert
        var provider = services.BuildServiceProvider();
        Assert.IsType<Broadcaster>(provider.GetRequiredKeyedService<IBroadcaster>("sitecore"));
        Assert.Null(provider.GetService<IBroadcaster>());
        Assert.Null(provider.GetService<IBroadcastTransport>());
    }

    [Fact]
    public async Task AddKeyedRedisBroadcaster_WhenBroadcasting_ShouldPublishUnderItsOwnPrefix()
    {
        // Arrange
        var channel = Guid.NewGuid().ToString("N");
        var queue = await fixture.Multiplexer
            .GetSubscriber()
            .SubscribeAsync(RedisChannel.Literal($"sitecore:{typeof(string).FullName}:{channel}"));
        var services = new ServiceCollection();
        services.AddSingleton(fixture.Multiplexer);
        services.AddRedisBroadcaster("orders");
        services.AddKeyedRedisBroadcaster(
            serviceKey: "sitecore",
            keyPrefix: "sitecore"
        );
        var broadcaster = services
            .BuildServiceProvider()
            .GetRequiredKeyedService<IBroadcaster>("sitecore");

        // Act
        await broadcaster.BroadcastAsync(channel, "payload", TestContext.Current.CancellationToken);

        // Assert
        var message = await queue
            .ReadAsync(TestContext.Current.CancellationToken)
            .AsTask()
            .WaitAsync(_timeout, TestContext.Current.CancellationToken);
        Assert.Equal("\"payload\"", (string?)message.Message);

        await queue.UnsubscribeAsync();
    }
}
