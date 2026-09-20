using System.Net;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;
using Tedd.Quicly.Transport.MsQuic.WebTransport;

namespace Tedd.Quicly.Transport.MsQuic.Tests.WebTransport;

/// <summary>The connector's own behaviour: argument checks, the authority it falls back to, and what it owns.</summary>
public class WebTransportConnectorTests
{
    [Theory]
    [InlineData("127.0.0.1", 4433, "127.0.0.1:4433")]
    [InlineData("::1", 443, "[::1]:443")]
    public void The_fallback_authority_of_an_ip_endpoint_is_its_address_and_port(string address, int port, string expected) =>
        Assert.Equal(expected, WebTransportConnector.DescribeEndpoint(new IPEndPoint(IPAddress.Parse(address), port)));

    [Fact]
    public void The_fallback_authority_of_a_dns_endpoint_is_its_host_and_port() =>
        Assert.Equal("game.example:4433", WebTransportConnector.DescribeEndpoint(new DnsEndPoint("game.example", 4433)));

    [Fact]
    public void An_endpoint_of_another_kind_falls_back_to_localhost() =>
        Assert.Equal("localhost", WebTransportConnector.DescribeEndpoint(new CustomEndPoint()));

    [Fact]
    public void A_null_inner_connector_is_refused() =>
        Assert.Throws<ArgumentNullException>(() => new WebTransportConnector(null!));

    [Fact]
    public void Invalid_options_are_refused_when_the_connector_is_built()
    {
        using var network = new SimulatedNetwork(new Core.Time.VirtualClock(), 1);
        Assert.Throws<ArgumentException>(() => new WebTransportConnector(new SimulatedConnector(network), new WebTransportOptions { Path = "nope" }));
    }

    [Fact]
    public void Connect_checks_its_arguments()
    {
        using var network = new SimulatedNetwork(new Core.Time.VirtualClock(), 1);
        using var connector = new WebTransportConnector(new SimulatedConnector(network));
        Assert.Throws<ArgumentNullException>(() => connector.Connect(null!, "localhost", new RecordingSink()));
        Assert.Throws<ArgumentNullException>(() => connector.Connect(new IPEndPoint(IPAddress.Loopback, 1), "localhost", null!));
    }

    [Fact]
    public void Disposing_twice_is_harmless_and_only_touches_an_inner_it_owns()
    {
        using var network = new SimulatedNetwork(new Core.Time.VirtualClock(), 1);
        var inner = new CountingConnector();
        var borrowed = new WebTransportConnector(inner);
        borrowed.Dispose();
        borrowed.Dispose();
        Assert.Equal(0, inner.Disposals);

        var owned = new WebTransportConnector(inner, ownsInner: true);
        owned.Dispose();
        owned.Dispose();
        Assert.Equal(1, inner.Disposals);
    }

    [Fact]
    public void A_null_inner_listener_is_refused() =>
        Assert.Throws<ArgumentNullException>(() => new WebTransportListener(null!));

    [Fact]
    public void The_listener_checks_its_start_arguments()
    {
        using var network = new SimulatedNetwork(new Core.Time.VirtualClock(), 1);
        using var simulated = new SimulatedListener(network, new IPEndPoint(IPAddress.Loopback, 0));
        using var listener = new WebTransportListener(simulated);
        Assert.Throws<ArgumentNullException>(() => listener.Start(null!, static (ITransport _, in NewConnectionInfo _) => null));
        Assert.Throws<ArgumentNullException>(() => listener.Start(static (in NewConnectionInfo _) => PreHandshakeDecision.Accept, null!));
        Assert.Equal(simulated.LocalEndPoint, listener.LocalEndPoint);
        listener.Stop();
    }

    private sealed class CustomEndPoint : EndPoint
    {
    }

    private sealed class CountingConnector : ITransportConnector, IDisposable
    {
        public int Disposals { get; private set; }

        public ITransport Connect(EndPoint endpoint, string? serverName, ITransportSink sink) => throw new NotSupportedException();

        public void Dispose() => Disposals++;
    }
}
