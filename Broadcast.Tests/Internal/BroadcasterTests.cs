using Broadcast.Internal;
using Xunit;

namespace Broadcast.Tests.Internal;

public sealed class BroadcasterTests
{
    private static readonly TimeSpan _timeout = TimeSpan.FromSeconds(5);

    [Fact]
    public async Task Stream_WhenBroadcast_ShouldYieldDeserializedMessage()
    {
        // Arrange
        var broadcaster = new Broadcaster(new InProcessBroadcast());
        using var cancellation = new CancellationTokenSource();
        await using var stream = broadcaster
            .Stream<Message>("topic", cancellation.Token)
            .GetAsyncEnumerator(cancellation.Token);

        var move = stream.MoveNextAsync().AsTask();

        // Act
        await broadcaster.BroadcastAsync("topic", new Message(7, "hi"), TestContext.Current.CancellationToken);
        var moved = await move.WaitAsync(_timeout, TestContext.Current.CancellationToken);
        var current = stream.Current;

        // Assert
        Assert.True(moved);
        Assert.Equal(new Message(7, "hi"), current);

        cancellation.Cancel();
    }

    [Fact]
    public async Task Stream_WhenBroadcastToDifferentTopic_ShouldNotReceive()
    {
        // Arrange
        var broadcaster = new Broadcaster(new InProcessBroadcast());
        using var cancellation = new CancellationTokenSource();
        await using var stream = broadcaster
            .Stream<Message>("a", cancellation.Token)
            .GetAsyncEnumerator(cancellation.Token);
        var move = stream.MoveNextAsync().AsTask();

        // Act
        await broadcaster.BroadcastAsync("b", new Message(1, "x"), TestContext.Current.CancellationToken);
        var winner = await Task.WhenAny(move, Task.Delay(TimeSpan.FromMilliseconds(200), TestContext.Current.CancellationToken));

        // Assert
        Assert.NotSame(move, winner);

        cancellation.Cancel();
        await Drain(move);
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

    private sealed record Message(int Id, string Status);
}
