using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Session;

namespace Tedd.Quicly.Client;

/// <summary>
/// Configuration of one <see cref="QuiclyClient.ConnectAsync"/>. Read when the connect starts: later changes to this instance
/// do not affect the connection (use <see cref="QuiclyClient.AuthToken"/> to change the token used by later reconnects).
/// </summary>
public sealed class ClientOptions
{
    /// <summary>The channel table; its hash must equal the server's (PROTOCOL.md §1). Required.</summary>
    public ChannelTable? Channels { get; set; }

    /// <summary>
    /// Options of every peer the client creates (copied once). To resume a session from an earlier run, set
    /// <see cref="PeerOptions.SessionToken"/> and <see cref="PeerOptions.LastEpoch"/>; the client manages both across its own
    /// reconnects. <see cref="PeerOptions.AutoFlushInterval"/> is implemented by <see cref="QuiclyClient.Poll"/>.
    /// </summary>
    public PeerOptions PeerOptions { get; set; } = new();

    /// <summary>The application's authentication token, sent in every Hello (at most 4 096 bytes; copied).</summary>
    public ReadOnlyMemory<byte> AuthToken { get; set; }

    /// <summary>The TLS server name (SNI), or <see langword="null"/>.</summary>
    public string? ServerName { get; set; }

    /// <summary>How to reconnect after a lost connection; <see langword="null"/> (the default) never reconnects.</summary>
    public ReconnectPolicy? Reconnect { get; set; }

    /// <summary>
    /// Called with every peer the client creates — the first connection, and a reconnect attempt that starts a fresh session
    /// — before its handshake completes: register message handlers here so they exist on every connection. A reconnect that
    /// resumes the session keeps the peer (<see cref="QuiclyPeer.Reconnect"/>), so its handlers, <see cref="QuiclyPeer.Tag"/>
    /// and statistics survive and this is <em>not</em> called again. Runs on the thread driving the client.
    /// </summary>
    public Action<QuiclyPeer>? PeerCreated { get; set; }

    internal void Validate()
    {
        if (Channels is null)
        {
            throw new ArgumentException("A channel table is required.", nameof(Channels));
        }

        if (PeerOptions is null)
        {
            throw new ArgumentException("Peer options are required.", nameof(PeerOptions));
        }

        if (AuthToken.Length > ControlCodec.MaxTokenLength)
        {
            throw new ArgumentException("An auth token is at most " + ControlCodec.MaxTokenLength + " bytes.", nameof(AuthToken));
        }

        Reconnect?.Validate();
    }
}
