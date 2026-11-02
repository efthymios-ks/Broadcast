namespace Broadcast.Internal;

internal sealed class InProcessBroadcast : FanOutTransportBase
{
    public override ValueTask BroadcastAsync(
        string channel,
        string payload,
        CancellationToken cancellationToken = default
    )
    {
        Publish(channel, payload);
        return ValueTask.CompletedTask;
    }

    protected override ValueTask<IAsyncDisposable?> ConnectUpstreamAsync(
        string channel,
        CancellationToken cancellationToken
    ) => ValueTask.FromResult<IAsyncDisposable?>(null);
}
