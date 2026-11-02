using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace Broadcast.Internal;

internal abstract class FanOutTransportBase : IBroadcastTransport
{
    private readonly ConcurrentDictionary<string, Topic> _topics = new();

    public async IAsyncEnumerable<string> Stream(
        string channel,
        [EnumeratorCancellation] CancellationToken cancellationToken
    )
    {
        var topic = _topics.GetOrAdd(channel, _ => new Topic());
        var (id, buffer) = await SubscribeAsync(topic, channel, cancellationToken);

        try
        {
            await foreach (var payload in buffer.Reader.ReadAllAsync(cancellationToken))
            {
                yield return payload;
            }
        }
        finally
        {
            await UnsubscribeAsync(topic, id);
        }
    }

    /// <summary>
    /// Registers a subscriber's buffer on the topic, opening the upstream subscription if this
    /// is the channel's first subscriber. Rolls the subscriber back if the upstream fails to open.
    /// </summary>
    private async Task<(Guid Id, Channel<string> Buffer)> SubscribeAsync(
        Topic topic,
        string channel,
        CancellationToken cancellationToken
    )
    {
        var id = Guid.NewGuid();
        var buffer = Channel.CreateUnbounded<string>();

        await topic.Gate.WaitAsync(cancellationToken);
        try
        {
            topic.Subscribers[id] = buffer;
            if (topic.Subscribers.Count == 1)
            {
                topic.Upstream = await ConnectUpstreamAsync(channel, cancellationToken);
            }
        }
        catch
        {
            topic.Subscribers.TryRemove(id, out _);
            throw;
        }
        finally
        {
            topic.Gate.Release();
        }

        return (id, buffer);
    }

    /// <summary>
    /// Removes a subscriber from the topic, closing the upstream subscription if it was the
    /// channel's last subscriber. The empty topic slot is intentionally kept.
    /// </summary>
    private static async Task UnsubscribeAsync(Topic topic, Guid id)
    {
        await topic.Gate.WaitAsync(CancellationToken.None);
        try
        {
            topic.Subscribers.TryRemove(id, out _);
            if (topic.Subscribers.IsEmpty && topic.Upstream is not null)
            {
                await topic.Upstream.DisposeAsync();
                topic.Upstream = null;
            }
        }
        finally
        {
            topic.Gate.Release();
        }
    }

    public abstract ValueTask BroadcastAsync(
        string channel,
        string payload,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Opens the single upstream subscription when a channel gains its first subscriber, and
    /// returns the handle disposed when the last subscriber leaves. <br/>
    /// Returns <c>null</c> when the transport has no upstream (in-process). <br/>
    /// The upstream's message handler should call <see cref="Publish"/> to fan a received
    /// message out to the local subscribers.
    /// </summary>
    protected abstract ValueTask<IAsyncDisposable?> ConnectUpstreamAsync(
        string channel,
        CancellationToken cancellationToken
    );

    protected void Publish(string channel, string payload)
    {
        if (_topics.TryGetValue(channel, out var topic))
        {
            foreach (var subscriber in topic.Subscribers.Values)
            {
                subscriber.Writer.TryWrite(payload);
            }
        }
    }

    private sealed class Topic
    {
        public ConcurrentDictionary<Guid, Channel<string>> Subscribers { get; } = new();
        public SemaphoreSlim Gate { get; } = new(1, 1);
        public IAsyncDisposable? Upstream { get; set; }
    }
}
