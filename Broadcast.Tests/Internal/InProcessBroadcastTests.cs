using Broadcast.Internal;
using Xunit;

namespace Broadcast.Tests.Internal;

public sealed class InProcessBroadcastTests
{
    private static readonly TimeSpan _timeout = TimeSpan.FromSeconds(5);

    [Fact]
    public async Task Stream_WhenBroadcast_ShouldYieldPayload()
    {
        // Arrange
        var transport = new InProcessBroadcast();
        using var cancellation = new CancellationTokenSource();
        await using var stream = transport
            .Stream("topic", cancellation.Token)
            .GetAsyncEnumerator(cancellation.Token);
        var move = stream.MoveNextAsync().AsTask();

        // Act
        await transport.BroadcastAsync("topic", "hello", TestContext.Current.CancellationToken);
        var moved = await move.WaitAsync(_timeout, TestContext.Current.CancellationToken);
        var current = stream.Current;

        // Assert
        Assert.True(moved);
        Assert.Equal("hello", current);

        cancellation.Cancel();
    }

    [Fact]
    public async Task BroadcastAsync_WhenMultipleSubscribers_ShouldFanOutToAll()
    {
        // Arrange
        var transport = new InProcessBroadcast();
        using var cancellation = new CancellationTokenSource();
        await using var first = transport
            .Stream("topic", cancellation.Token)
            .GetAsyncEnumerator(cancellation.Token);
        await using var second = transport
            .Stream("topic", cancellation.Token)
            .GetAsyncEnumerator(cancellation.Token);
        var firstMove = first.MoveNextAsync().AsTask();
        var secondMove = second.MoveNextAsync().AsTask();

        // Act
        await transport.BroadcastAsync("topic", "hello", TestContext.Current.CancellationToken);
        var firstMoved = await firstMove.WaitAsync(_timeout, TestContext.Current.CancellationToken);
        var secondMoved = await secondMove.WaitAsync(_timeout, TestContext.Current.CancellationToken);
        var firstCurrent = first.Current;
        var secondCurrent = second.Current;

        // Assert
        Assert.True(firstMoved);
        Assert.True(secondMoved);
        Assert.Equal("hello", firstCurrent);
        Assert.Equal("hello", secondCurrent);

        cancellation.Cancel();
    }

    [Fact]
    public async Task BroadcastAsync_WhenNoSubscribers_ShouldNotThrow()
    {
        // Arrange
        var transport = new InProcessBroadcast();

        // Act
        var exception = await Record
            .ExceptionAsync(() => transport.BroadcastAsync("topic", "x", TestContext.Current.CancellationToken)
            .AsTask());

        // Assert
        Assert.Null(exception);
    }
}
