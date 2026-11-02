using NSubstitute;
using Xunit;

namespace Broadcast.Tests;

public sealed class StreamExtensionsTests
{
    private static readonly TimeSpan _timeout = TimeSpan.FromSeconds(5);

    [Fact]
    public async Task ToAsyncEnumerable_WhenEnumerated_ShouldYieldAllItems()
    {
        // Arrange
        var items = new[] { 1, 2, 3 };

        // Act
        var result = new List<int>();
        await foreach (var item in items.ToAsyncEnumerable(TestContext.Current.CancellationToken))
        {
            result.Add(item);
        }

        // Assert
        Assert.Equal(items, result);
    }

    [Fact]
    public async Task ToAsyncEnumerable_WhenCancelled_ShouldNotThrow()
    {
        // Arrange
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        // Act
        var exception = await Record.ExceptionAsync(async () =>
        {
            await foreach (var _ in new[] { 1, 2 }.ToAsyncEnumerable(cancellation.Token))
            {
            }
        });

        // Assert
        Assert.Null(exception);
    }

    [Fact]
    public async Task Merge_WhenSourcesComplete_ShouldYieldAllItemsAndComplete()
    {
        // Arrange
        var first = new[] { 1, 2 }.ToAsyncEnumerable(TestContext.Current.CancellationToken);
        var second = new[] { 3, 4 }.ToAsyncEnumerable(TestContext.Current.CancellationToken);

        // Act
        var result = new List<int>();
        await foreach (var item in first.Merge(second, TestContext.Current.CancellationToken))
        {
            result.Add(item);
        }

        // Assert
        Assert.Equal([1, 2, 3, 4], result.OrderBy(value => value));
    }

    [Fact]
    public async Task Pulse_WhenFireImmediately_ShouldTickImmediatelyThenOnInterval()
    {
        // Arrange
        var time = CreateControllableTime(out var fireTick);
        using var cancellation = new CancellationTokenSource();
        await using var pulse = time
            .Pulse(TimeSpan.FromSeconds(5), fireImmediately: true, cancellation.Token)
            .GetAsyncEnumerator(cancellation.Token);

        // Act
        var immediateMoved = await pulse.MoveNextAsync().AsTask().WaitAsync(_timeout, TestContext.Current.CancellationToken);
        var immediate = pulse.Current;
        var next = pulse.MoveNextAsync().AsTask();
        fireTick();
        var nextMoved = await next.WaitAsync(_timeout, TestContext.Current.CancellationToken);

        // Assert
        Assert.True(immediateMoved);
        Assert.Equal(DateTimeOffset.UnixEpoch, immediate);
        Assert.True(nextMoved);
        Assert.Equal(DateTimeOffset.UnixEpoch, pulse.Current);

        cancellation.Cancel();
        await Drain(next);
    }

    [Fact]
    public async Task Poll_WhenFireImmediately_ShouldFetchImmediatelyThenEachInterval()
    {
        // Arrange
        var time = CreateControllableTime(out var fireTick);
        var calls = 0;
        Task<IEnumerable<int>> Fetch(CancellationToken ct)
        {
            calls++;
            return Task.FromResult<IEnumerable<int>>([calls]);
        }

        using var cancellation = new CancellationTokenSource();
        await using var poll = time
            .Poll(Fetch, every: TimeSpan.FromSeconds(5), fireImmediately: true, cancellationToken: cancellation.Token)
            .GetAsyncEnumerator(cancellation.Token);

        // Act
        var firstMoved = await poll.MoveNextAsync().AsTask().WaitAsync(_timeout, TestContext.Current.CancellationToken);
        var firstValue = poll.Current;
        var next = poll.MoveNextAsync().AsTask();
        fireTick();
        var nextMoved = await next.WaitAsync(_timeout, TestContext.Current.CancellationToken);
        var secondValue = poll.Current;

        // Assert
        Assert.True(firstMoved);
        Assert.Equal(1, firstValue);
        Assert.True(nextMoved);
        Assert.Equal(2, secondValue);

        cancellation.Cancel();
        await Drain(next);
    }

    private static TimeProvider CreateControllableTime(out Action fireTick)
    {
        var time = Substitute.For<TimeProvider>();
        time.GetUtcNow().Returns(DateTimeOffset.UnixEpoch);

        TimerCallback callback = null!;
        object? state = null;
        time.CreateTimer(
                Arg.Do<TimerCallback>(value => callback = value),
                Arg.Do<object?>(value => state = value),
                Arg.Any<TimeSpan>(),
                Arg.Any<TimeSpan>())
            .Returns(Substitute.For<ITimer>());

        fireTick = () => callback(state);

        return time;
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
