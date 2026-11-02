# Broadcast

A transport-agnostic, strongly-typed fan-out bus for .NET.
Every subscriber on a channel receives every message (pub/sub, **not** a queue).
Pairs cleanly with .NET 10's `TypedResults.ServerSentEvents` for SSE endpoints.

## Project layout

```
Broadcast                 contracts, in-process transport, stream composition
Broadcast.Redis           Redis pub/sub transport for multi-pod fan-out
Broadcast.Tests           xUnit v3 tests for the contracts, the in-process transport and stream composition
Broadcast.Redis.Tests     xUnit v3 tests for the Redis transport, on a Redis Testcontainer
```

## Core types

| Type | Role |
|---|---|
| `IBroadcaster` | Strongly-typed fan-out bus. `Stream<T>` subscribes, `BroadcastAsync<T>` publishes. |
| `IBroadcastTransport` | String-payload transport (in-process or Redis). Swappable. |
| `FanOutTransportBase` | Internal base that multiplexes one upstream subscription per channel. |
| `StreamExtensions` | `ToAsyncEnumerable`, `Merge`, `Pulse`, `Poll` for composing live streams. |

## Namespaces

```csharp
using Broadcast;         // IBroadcaster, StreamExtensions
using Broadcast.Redis;   // AddRedisBroadcaster
```

## Setup

```csharp
// In-process (single pod / dev)
services.AddBroadcaster();

// Redis pub/sub (multi-pod). Requires an IConnectionMultiplexer in DI.
services.AddRedisBroadcaster("orders:");

// A library's own Redis broadcaster, resolved only under its key, with its own channel prefix
services.AddKeyedRedisBroadcaster(serviceKey: "sitecore", keyPrefix: "sitecore:changes");

// Custom transport
services.TryAddSingleton<MyTransport>(); // Your IBroadcastTransport implementation
services.AddBroadcaster<MyTransport>();
```

Both overloads register `TimeProvider.System` (used by `Poll` / `Pulse`).

## Publishing

```csharp
public sealed class ShipOrderEndpoint
{
    private static async Task<IResult> HandleAsync(
        int orderId,
        IBroadcaster broadcaster,
        CancellationToken cancellationToken
    )
    {
        await broadcaster.BroadcastAsync(
            "orders", 
            new OrderUpdate(orderId, "Shipped"), 
            cancellationToken
        );
        return Results.Accepted();
    }
}
```

## Subscribing (SSE)

```csharp
public sealed class StreamOrdersEndpoint
{
    private static IResult HandleAsync(
        IBroadcaster broadcaster,
        TimeProvider time,
        CancellationToken cancellationToken
    )
    {
        var poll = time.Poll(
            Fetch, 
            every: TimeSpan.FromSeconds(5), 
            fireImmediately: true, 
            cancellationToken: cancellationToken
        );
        var live = broadcaster.Stream<OrderUpdate>("orders", cancellationToken);
        return TypedResults.ServerSentEvents(poll.Merge(live, cancellationToken), eventType: "order-update");
    }

    private static Task<IEnumerable<OrderUpdate>> Fetch(CancellationToken cancellationToken) => /* DB read */ ...;
}
```

A browser consumes it with `EventSource`:

```js
const source = new EventSource("/api/orders/feed");
source.addEventListener("order-update", (e) => console.log(JSON.parse(e.data)));
```

## Multiplexing

Subscriptions are multiplexed **per pod per channel**.
The first `Stream<T>` call for a channel opens a single underlying subscription (for Redis,
one channel + handler).
Additional streams fan out from it locally.
The last subscriber to leave closes it.

So 1000 concurrent streams of one channel on a pod cost **one** Redis subscription, not 1000.

## Stream composition

Sources are plain `IAsyncEnumerable<T>`, composed at the endpoint via `StreamExtensions`:

| Method | Role |
|---|---|
| `ToAsyncEnumerable()` | Wrap an `IEnumerable<T>` (e.g. a one-shot DB read). |
| `Merge(other)` | Run two streams in parallel (catch-up poll + live tail). |
| `time.Pulse(period)` | `TimeProvider`-backed periodic tick. |
| `time.Poll(fetch, every)` | Periodic fetch, each result flattened into the stream. |

Both `Pulse` and `Poll` take a `TimeProvider`, so tests can drive time via a substituted provider.

## Transports

| Transport | Use case |
|---|---|
| `InProcessBroadcast` (default) | Single process. No external dependencies. Very fast. |
| `RedisBroadcast` | Multi-pod fan-out via Redis pub/sub. The `keyPrefix` lets one Redis serve many apps. |
| Custom `IBroadcastTransport` | Any upstream, registered with `AddBroadcaster<TTransport>()`. |

Only the transport is swappable.
`IBroadcaster` and its JSON (de)serialization are shared across all transports.

## SSE caveat

**Response body must not be buffered.**
SSE delivers frames incrementally by flushing the response as it streams.
Any middleware that swaps `HttpContext.Response.Body` for a buffering stream
(e.g. a request-logging middleware that captures the response into a `MemoryStream`)
traps every frame in that buffer — nothing reaches the client until the connection closes.
If you capture the response body, **tee** it: write straight through to the original body
and keep only a bounded copy for logging.

## License

MIT.
