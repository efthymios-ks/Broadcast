using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace Broadcast;

public static class StreamExtensions
{
    public static async IAsyncEnumerable<TElement> ToAsyncEnumerable<TElement>(
        this IEnumerable<TElement> source,
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        foreach (var item in source)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                yield break;
            }

            yield return item;
        }
    }

    public static async IAsyncEnumerable<TElement> Merge<TElement>(
        this IAsyncEnumerable<TElement> first,
        IAsyncEnumerable<TElement> second,
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        var output = Channel.CreateUnbounded<TElement>();

        var pumps = Task.WhenAll(
            Pump(first, output.Writer, cancellationToken),
            Pump(second, output.Writer, cancellationToken)
        );

        _ = pumps.ContinueWith(_ => output.Writer.TryComplete(), TaskScheduler.Default);

        await foreach (var item in output.Reader.ReadAllAsync(cancellationToken))
        {
            yield return item;
        }

        // Surface source exceptions
        await pumps;

        static async Task Pump(
            IAsyncEnumerable<TElement> source,
            ChannelWriter<TElement> writer,
            CancellationToken cancellationToken
        )
        {
            await foreach (var item in source.WithCancellation(cancellationToken))
            {
                await writer.WriteAsync(item, cancellationToken);
            }
        }
    }

    public static async IAsyncEnumerable<DateTimeOffset> Pulse(
        this TimeProvider time,
        TimeSpan period,
        bool fireImmediately = true,
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        if (fireImmediately)
        {
            yield return time.GetUtcNow();
        }

        using var timer = new PeriodicTimer(period, time);
        while (await timer.WaitForNextTickAsync(cancellationToken))
        {
            yield return time.GetUtcNow();
        }
    }

    public static async IAsyncEnumerable<TElement> Poll<TElement>(
        this TimeProvider time,
        Func<CancellationToken, Task<IEnumerable<TElement>>> fetch,
        TimeSpan every,
        bool fireImmediately = true,
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        await foreach (var _ in time.Pulse(every, fireImmediately, cancellationToken))
        {
            foreach (var item in await fetch(cancellationToken))
            {
                yield return item;
            }
        }
    }
}
