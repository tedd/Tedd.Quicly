using System.Net;
using System.Security.Cryptography.X509Certificates;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Transport.MsQuic.WebTransport;

/// <summary>
/// Accepts WebTransport-over-HTTP/3 sessions: every connection an inner listener accepts is wrapped in a
/// <see cref="WebTransportTransport"/>, which answers the Extended CONNECT request and only then shows the connection
/// to the application.
/// </summary>
/// <remarks>
/// The accept callback sees the carrier, not the raw QUIC transport, so an application binds to it exactly as it binds
/// to <see cref="MsQuicTransportListener"/>. <see cref="CreateMsQuic"/> builds the usual MsQuic listener with the
/// <c>h3</c> ALPN.
/// </remarks>
public sealed class WebTransportListener : ITransportListener
{
    private readonly ITransportListener _inner;
    private readonly WebTransportOptions _options;
    private readonly bool _ownsInner;
    private int _disposed;
    private long _sessionsRefused;

    /// <summary>Wraps <paramref name="inner"/>, which must accept the <c>h3</c> ALPN.</summary>
    /// <param name="inner">The raw-QUIC listener.</param>
    /// <param name="options">Carrier options; copied and validated.</param>
    /// <param name="ownsInner">True when disposing this listener disposes <paramref name="inner"/> too.</param>
    public WebTransportListener(ITransportListener inner, WebTransportOptions? options = null, bool ownsInner = false)
    {
        ArgumentNullException.ThrowIfNull(inner);
        _inner = inner;
        _options = (options ?? new WebTransportOptions()).Clone();
        _options.Validate();
        _ownsInner = ownsInner;
    }

    /// <summary>
    /// Builds a listener over MsQuic with the <c>h3</c> ALPN. <paramref name="transportOptions"/> is copied; its
    /// ALPN list is replaced and its stream limits are raised by what HTTP/3 needs for itself.
    /// </summary>
    /// <param name="localEndPoint">The address to bind.</param>
    /// <param name="certificate">The server certificate, with its private key.</param>
    /// <param name="transportOptions">MsQuic options.</param>
    /// <param name="options">Carrier options.</param>
    /// <param name="registration">An existing MsQuic registration to share, or null for a private one.</param>
    public static WebTransportListener CreateMsQuic(
        IPEndPoint localEndPoint,
        X509Certificate2 certificate,
        MsQuicTransportOptions? transportOptions = null,
        WebTransportOptions? options = null,
        MsQuicRegistration? registration = null)
    {
        WebTransportOptions carrier = (options ?? new WebTransportOptions()).Clone();
        carrier.Validate();
        MsQuicTransportOptions transport = WebTransportConnector.PrepareTransportOptions(transportOptions, carrier);
        return new WebTransportListener(new MsQuicTransportListener(localEndPoint, certificate, transport, registration), carrier, ownsInner: true);
    }

    /// <inheritdoc/>
    public IPEndPoint LocalEndPoint => _inner.LocalEndPoint;

    /// <summary>Connections refused because the application's accept callback returned null.</summary>
    public long SessionsRefusedCount => Interlocked.Read(ref _sessionsRefused);

    /// <inheritdoc/>
    /// <remarks>
    /// <paramref name="accept"/> is called with the carrier while the inner listener admits the connection, under the
    /// same rule as any accept callback: keep the transport, do not call it yet.
    /// </remarks>
    public void Start(PreHandshakeCallback preHandshake, AcceptCallback accept)
    {
        ArgumentNullException.ThrowIfNull(preHandshake);
        ArgumentNullException.ThrowIfNull(accept);

        _inner.Start(preHandshake, (ITransport inner, in NewConnectionInfo info) =>
        {
            var carrier = new WebTransportTransport(_options, isClient: false);

            // No callback of the inner transport can run before this returns, so attaching here is race-free.
            carrier.AttachInner(inner);
            ITransportSink? sink = accept(carrier, in info);
            if (sink is null)
            {
                Interlocked.Increment(ref _sessionsRefused);
                carrier.Dispose();
                return null;
            }

            carrier.SetSink(sink);
            return carrier;
        });
    }

    /// <inheritdoc/>
    public void Stop() => _inner.Stop();

    /// <summary>Stops the listener and disposes the inner one when this listener owns it.</summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        if (_ownsInner) _inner.Dispose();
    }
}
