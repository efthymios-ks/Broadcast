namespace Broadcast;

public interface IBroadcaster
{
    IAsyncEnumerable<TMessage> Stream<TMessage>(string channel, CancellationToken cancellationToken);

    ValueTask BroadcastAsync<TMessage>(string channel, TMessage message, CancellationToken cancellationToken = default);
}
