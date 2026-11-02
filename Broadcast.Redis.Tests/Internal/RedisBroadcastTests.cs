using Broadcast.Redis.Internal;
using StackExchange.Redis;
using Xunit;

namespace Broadcast.Redis.Tests.Internal;

public sealed class RedisBroadcastTests(RedisFixture fixture)
{
    private const string Prefix = "test";
    private static readonly TimeSpan _timeout = TimeSpan.FromSeconds(5);

    [Fact]
    public async Task BroadcastAsync_WhenCalled_ShouldPublishToPrefixedChannel()
    {
        // Arrange
        var channel = Guid.NewGuid().ToString("N");
        var redisChannel = RedisChannel.Literal($"{Prefix}:{channel}");
        var received = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscriber = fixture.Multiplexer.GetSubscriber();
        await subscriber.SubscribeAsync(redisChannel, (_, value) => received.TrySetResult(value!));
        var transport = new RedisBroadcast(fixture.Multiplexer, $"{Prefix}:");

        // Act
        await transport.BroadcastAsync(channel, "payload", TestContext.Current.CancellationToken);
        var payload = await received.Task.WaitAsync(_timeout, TestContext.Current.CancellationToken);

        // Assert
        Assert.Equal("payload", payload);

        await subscriber.UnsubscribeAsync(redisChannel);
    }

    [Fact]
    public async Task Stream_WhenUpstreamPublishes_ShouldYieldPayload()
    {
        // Arrange
        var channel = Guid.NewGuid().ToString("N");
        var transport = new RedisBroadcast(fixture.Multiplexer, $"{Prefix}:");
        using var cancellation = new CancellationTokenSource();
        await using var stream = transport
            .Stream(channel, cancellation.Token)
            .GetAsyncEnumerator(cancellation.Token);
        var move = stream.MoveNextAsync().AsTask();
        await WaitForSubscriberCount(channel, 1);

        // Act
        await fixture.Multiplexer
            .GetSubscriber()
            .PublishAsync(RedisChannel.Literal($"{Prefix}:{channel}"), "payload");
        var moved = await move.WaitAsync(_timeout, TestContext.Current.CancellationToken);

        // Assert
        Assert.True(moved);
        Assert.Equal("payload", stream.Current);

        cancellation.Cancel();
    }

    [Fact]
    public async Task Stream_WhenMultipleSubscribersSameChannel_ShouldSubscribeOnceAndFanOutToAll()
    {
        // Arrange
        var channel = Guid.NewGuid().ToString("N");
        var transport = new RedisBroadcast(fixture.Multiplexer, $"{Prefix}:");
        using var cancellation = new CancellationTokenSource();
        await using var first = transport
            .Stream(channel, cancellation.Token)
            .GetAsyncEnumerator(cancellation.Token);
        await using var second = transport
            .Stream(channel, cancellation.Token)
            .GetAsyncEnumerator(cancellation.Token);
        var firstMove = first.MoveNextAsync().AsTask();
        var secondMove = second.MoveNextAsync().AsTask();
        await WaitForSubscriberCount(channel, 1);

        // Act
        await fixture.Multiplexer
            .GetSubscriber()
            .PublishAsync(RedisChannel.Literal($"{Prefix}:{channel}"), "payload");
        var firstMoved = await firstMove.WaitAsync(_timeout, TestContext.Current.CancellationToken);
        var secondMoved = await secondMove.WaitAsync(_timeout, TestContext.Current.CancellationToken);
        var subscriberCount = await GetSubscriberCount(channel);

        // Assert
        Assert.Equal(1, subscriberCount);
        Assert.True(firstMoved);
        Assert.True(secondMoved);
        Assert.Equal("payload", first.Current);
        Assert.Equal("payload", second.Current);

        cancellation.Cancel();
    }

    [Fact]
    public async Task Stream_WhenLastSubscriberLeaves_ShouldUnsubscribeOnce()
    {
        // Arrange
        var channel = Guid.NewGuid().ToString("N");
        var transport = new RedisBroadcast(fixture.Multiplexer, $"{Prefix}:");
        using var firstCancellation = new CancellationTokenSource();
        using var secondCancellation = new CancellationTokenSource();
        var first = transport.Stream(channel, firstCancellation.Token).GetAsyncEnumerator(firstCancellation.Token);
        var second = transport.Stream(channel, secondCancellation.Token).GetAsyncEnumerator(secondCancellation.Token);
        var firstMove = first.MoveNextAsync().AsTask();
        var secondMove = second.MoveNextAsync().AsTask();
        await WaitForSubscriberCount(channel, 1);

        // Act
        firstCancellation.Cancel();
        await Drain(firstMove);
        await first.DisposeAsync();
        var afterFirst = await GetSubscriberCount(channel);

        secondCancellation.Cancel();
        await Drain(secondMove);
        await second.DisposeAsync();
        var afterSecond = await WaitForSubscriberCount(channel, 0);

        // Assert
        Assert.Equal(1, afterFirst);
        Assert.Equal(0, afterSecond);
    }

    [Fact]
    public async Task Stream_WhenAllLeaveThenNewSubscriberJoins_ShouldReopenUpstream()
    {
        // Arrange
        var channel = Guid.NewGuid().ToString("N");
        var transport = new RedisBroadcast(fixture.Multiplexer, $"{Prefix}:");

        using var firstCancellation = new CancellationTokenSource();
        var first = transport.Stream(channel, firstCancellation.Token).GetAsyncEnumerator(firstCancellation.Token);
        var firstMove = first.MoveNextAsync().AsTask();
        await WaitForSubscriberCount(channel, 1);

        // Act: all leave
        firstCancellation.Cancel();
        await Drain(firstMove);
        await first.DisposeAsync();
        var afterClose = await WaitForSubscriberCount(channel, 0);

        // Act: a new subscriber joins
        using var secondCancellation = new CancellationTokenSource();
        await using var second = transport
            .Stream(channel, secondCancellation.Token)
            .GetAsyncEnumerator(secondCancellation.Token);
        var secondMove = second.MoveNextAsync().AsTask();
        var afterReopen = await WaitForSubscriberCount(channel, 1);

        await fixture.Multiplexer
            .GetSubscriber()
            .PublishAsync(RedisChannel.Literal($"{Prefix}:{channel}"), "payload");
        var moved = await secondMove.WaitAsync(_timeout, TestContext.Current.CancellationToken);

        // Assert
        Assert.Equal(0, afterClose);
        Assert.Equal(1, afterReopen);
        Assert.True(moved);
        Assert.Equal("payload", second.Current);

        secondCancellation.Cancel();
    }

    [Fact]
    public async Task Stream_WhenPublishedToDifferentChannel_ShouldNotReceive()
    {
        // Arrange
        var channelA = Guid.NewGuid().ToString("N");
        var channelB = Guid.NewGuid().ToString("N");
        var transport = new RedisBroadcast(fixture.Multiplexer, $"{Prefix}:");
        using var cancellation = new CancellationTokenSource();
        await using var stream = transport
            .Stream(channelA, cancellation.Token)
            .GetAsyncEnumerator(cancellation.Token);
        var move = stream.MoveNextAsync().AsTask();
        await WaitForSubscriberCount(channelA, 1);

        // Act
        await fixture.Multiplexer
            .GetSubscriber()
            .PublishAsync(RedisChannel.Literal($"{Prefix}:{channelB}"), "payload");
        var winner = await Task.WhenAny(move, Task.Delay(TimeSpan.FromMilliseconds(500), TestContext.Current.CancellationToken));

        // Assert
        Assert.NotSame(move, winner);

        cancellation.Cancel();
        await Drain(move);
    }

    private async Task<long> GetSubscriberCount(string channel)
        => await fixture.Multiplexer
            .GetServers()
            .Single()
            .SubscriptionSubscriberCountAsync(RedisChannel.Literal($"{Prefix}:{channel}"));

    private async Task<long> WaitForSubscriberCount(string channel, long expected)
    {
        var deadline = DateTime.UtcNow + _timeout;
        long current = -1;
        while (DateTime.UtcNow < deadline)
        {
            current = await GetSubscriberCount(channel);
            if (current == expected)
            {
                return current;
            }

            await Task.Delay(50, TestContext.Current.CancellationToken);
        }

        return current;
    }

    private static async Task Drain(Task move)
    {
        try
        {
            await move;
        }
        catch (OperationCanceledException)
        {
        }
    }
}
