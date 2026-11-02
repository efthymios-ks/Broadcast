using Broadcast.Internal;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Broadcast.Tests;

public sealed class BroadcastExtensionsTests
{
    [Fact]
    public void AddBroadcaster_WhenCalled_ShouldRegisterInProcessTransportBroadcasterAndTimeProvider()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddBroadcaster();

        // Assert
        var provider = services.BuildServiceProvider();
        Assert.IsType<Broadcaster>(provider.GetRequiredService<IBroadcaster>());
        Assert.IsType<InProcessBroadcast>(provider.GetRequiredService<IBroadcastTransport>());
        Assert.NotNull(provider.GetRequiredService<TimeProvider>());
    }
}
