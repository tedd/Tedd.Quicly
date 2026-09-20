using System.Net;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Transport.MsQuic.WebTransport;

/// <summary>
/// Connects over WebTransport-over-HTTP/3: it makes a raw QUIC connection with the <c>h3</c> ALPN through an inner
/// connector and hands back a <see cref="WebTransportTransport"/> that establishes the session on it.
/// </summary>
/// <remarks>
/// Any <see cref="ITransportConnector"/> will do, which is what lets the carrier be tested over the simulated network;
/// <see cref="CreateMsQuic"/> builds the usual MsQuic one with the right ALPN and stream table.
/// </remarks>
public sealed class WebTransportConnector : ITransportConnector, IDisposable
{
    private readonly ITransportConnector _inner;
    private readonly WebTransportOptions _options;
    private readonly bool _ownsInner;
    private int _disposed;

    /// <summary>Wraps <paramref name="inner"/>, whose transports must negotiate the <c>h3</c> ALPN.</summary>
    /// <param name="inner">The raw-QUIC connector.</param>
    /// <param name="options">Carrier options; copied and validated.</param>
    /// <param name="ownsInner">True when disposing this connector disposes <paramref name="inner"/> too.</param>
    public WebTransportConnector(ITransportConnector inner, WebTransportOptions? options = null, bool ownsInner = false)
    {
        ArgumentNullException.ThrowIfNull(inner);
        _inner = inner;
        _options = (options ?? new WebTransportOptions()).Clone();
        _options.Validate();
        _ownsInner = ownsInner;
    }

    /// <summary>
    /// Builds a connector over MsQuic with the <c>h3</c> ALPN. <paramref name="transportOptions"/> is copied; its
    /// <see cref="MsQuicTransportOptions.Alpns"/> is replaced and its stream table is kept in step with
    /// <see cref="WebTransportOptions.MaxStreams"/>.
    /// </summary>
    /// <param name="transportOptions">MsQuic options (certificate validation, timeouts, stream limits).</param>
    /// <param name="options">Carrier options.</param>
    /// <param name="registration">An existing MsQuic registration to share, or null for a private one.</param>
    public static WebTransportConnector CreateMsQuic(MsQuicTransportOptions? transportOptions = null, WebTransportOptions? options = null, MsQuicRegistration? registration = null)
    {
        WebTransportOptions carrier = (options ?? new WebTransportOptions()).Clone();
        carrier.Validate();
        MsQuicTransportOptions transport = PrepareTransportOptions(transportOptions, carrier);
        return new WebTransportConnector(new MsQuicTransportConnector(transport, registration), carrier, ownsInner: true);
    }

    /// <summary>The <c>h3</c> ALPN and a stream table large enough for the carrier's mirror of it.</summary>
    internal static MsQuicTransportOptions PrepareTransportOptions(MsQuicTransportOptions? source, WebTransportOptions carrier)
    {
        MsQuicTransportOptions transport = (source ?? new MsQuicTransportOptions()).Clone();
        transport.Alpns = [WebTransportOptions.Alpn];
        if (transport.MaxStreams > carrier.MaxStreams) carrier.MaxStreams = transport.MaxStreams;

        // HTTP/3 needs the control and QPACK streams on top of whatever QUICLY asked for, from the first packet on.
        transport.ServerPeerUnidiStreamCount = Raise(transport.ServerPeerUnidiStreamCount, 3);
        transport.ClientPeerUnidiStreamCount = Raise(transport.ClientPeerUnidiStreamCount, 3);
        transport.ServerPeerBidiStreamCount = Raise(transport.ServerPeerBidiStreamCount, 1);
        return transport;
    }

    private static ushort Raise(ushort value, int extra)
    {
        int raised = value + extra;
        return raised >= ushort.MaxValue ? ushort.MaxValue : (ushort)raised;
    }

    /// <inheritdoc/>
    public ITransport Connect(EndPoint endpoint, string? serverName, ITransportSink sink)
    {
        ArgumentNullException.ThrowIfNull(endpoint);
        ArgumentNullException.ThrowIfNull(sink);

        WebTransportOptions options = _options;
        if (string.IsNullOrEmpty(options.Authority))
        {
            options = options.Clone();
            options.Authority = serverName ?? DescribeEndpoint(endpoint);
        }

        var carrier = new WebTransportTransport(options, isClient: true, sink);
        try
        {
            // The inner transport may raise a callback before Connect returns; the gate holds those until it is attached.
            lock (carrier.Gate)
            {
                ITransport inner = _inner.Connect(endpoint, serverName, carrier);
                carrier.AttachInner(inner);
            }
        }
        catch
        {
            carrier.Dispose();
            throw;
        }

        return carrier;
    }

    /// <summary>The <c>:authority</c> used when neither the options nor the caller named one.</summary>
    internal static string DescribeEndpoint(EndPoint endpoint) => endpoint switch
    {
        IPEndPoint ip and { AddressFamily: System.Net.Sockets.AddressFamily.InterNetworkV6 } => $"[{ip.Address}]:{ip.Port}",
        IPEndPoint ip => $"{ip.Address}:{ip.Port}",
        DnsEndPoint dns => $"{dns.Host}:{dns.Port}",
        _ => "localhost",
    };

    /// <summary>Disposes the inner connector when this one owns it.</summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        if (_ownsInner && _inner is IDisposable disposable) disposable.Dispose();
    }
}
