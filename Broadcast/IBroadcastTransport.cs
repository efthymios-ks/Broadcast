namespace Broadcast;

public interface IBroadcastTransport
{
    IAsyncEnumerable<string> Stream(
        string channel,
        CancellationToken cancellationToken
    );

    ValueTask BroadcastAsync(
        string channel,
        string payload,
        CancellationToken cancellationToken = default
    );
}
