using System.Buffers.Binary;
using System.Security.Cryptography;
using Tedd.Quicly.Core.Session;

namespace Tedd.Quicly.Server;

/// <summary>One session the server holds (PROTOCOL.md §4.1): its id, current epoch, live connection and grace deadline.</summary>
internal sealed class SessionRecord
{
    /// <summary>The random, non-zero session id.</summary>
    public ulong SessionId;

    /// <summary>The epoch of the most recent admission (1 for a fresh session); only a token of this epoch can resume.</summary>
    public uint Epoch;

    /// <summary>The live connection, or <see langword="null"/> while the session waits for a resume.</summary>
    public QuiclyPeer? Peer;

    /// <summary>The <see cref="QuiclyPeer.Tag"/> the session's last connection had (restored on the resuming connection).</summary>
    public ulong Tag;

    /// <summary>When the session was last admitted (resume rate cap).</summary>
    public long LastAdmittedMicros;

    /// <summary>When a disconnected session ends; <see cref="long.MaxValue"/> while connected.</summary>
    public long ExpiresMicros = long.MaxValue;

    /// <summary>Position in the registry's disconnected list, or -1.</summary>
    public int DisconnectedIndex = -1;
}

/// <summary>
/// The sessions of a server, keyed by id, with the disconnected ones in a list swept for expiry. Game thread only (the
/// admission policy and the close handling both run inside <see cref="QuiclyServer.PollAll"/>).
/// </summary>
internal sealed class SessionRegistry(int expectedSessions)
{
    private readonly Dictionary<ulong, SessionRecord> _byId = new(expectedSessions);
    private readonly List<SessionRecord> _disconnected = [];

    /// <summary>Sessions held (connected or within their grace period).</summary>
    public int Count => _byId.Count;

    /// <summary>Sessions waiting for a resume.</summary>
    public int DisconnectedCount => _disconnected.Count;

    /// <summary>The earliest grace deadline (may be stale-early after a resume; the next sweep recomputes it).</summary>
    public long EarliestExpiry { get; private set; } = long.MaxValue;

    /// <summary>The session with <paramref name="sessionId"/>, or <see langword="null"/>.</summary>
    public SessionRecord? Find(ulong sessionId) => _byId.GetValueOrDefault(sessionId);

    /// <summary>Creates a fresh session (epoch 1) for <paramref name="peer"/> with a new random id.</summary>
    public SessionRecord Create(QuiclyPeer peer, long nowMicros)
    {
        ulong id;
        do
        {
            id = NewId();
        }
        while (id == 0 || _byId.ContainsKey(id));

        SessionRecord record = new() { SessionId = id, Epoch = 1, Peer = peer, Tag = peer.Tag, LastAdmittedMicros = nowMicros };
        _byId.Add(id, record);
        return record;
    }

    /// <summary>Binds a resumed session to its new connection and epoch.</summary>
    public void Attach(SessionRecord record, QuiclyPeer peer, uint epoch, long nowMicros)
    {
        Detach(record);
        record.Peer = peer;
        record.Epoch = epoch;
        record.LastAdmittedMicros = nowMicros;
        record.ExpiresMicros = long.MaxValue;
    }

    /// <summary>The session lost its connection; it ends at <paramref name="expiresMicros"/> unless resumed.</summary>
    public void Disconnect(SessionRecord record, long expiresMicros)
    {
        record.Peer = null;
        record.ExpiresMicros = expiresMicros;
        if (record.DisconnectedIndex < 0)
        {
            record.DisconnectedIndex = _disconnected.Count;
            _disconnected.Add(record);
        }

        if (expiresMicros < EarliestExpiry)
        {
            EarliestExpiry = expiresMicros;
        }
    }

    /// <summary>Removes a session now.</summary>
    public void Remove(SessionRecord record)
    {
        Detach(record);
        record.Peer = null;
        _byId.Remove(record.SessionId);
    }

    /// <summary>Removes every disconnected session whose grace period ended and reports it in <paramref name="ended"/>.</summary>
    public void Sweep(long nowMicros, List<SessionEndInfo> ended)
    {
        long earliest = long.MaxValue;
        for (int i = _disconnected.Count - 1; i >= 0; i--)
        {
            SessionRecord record = _disconnected[i];
            if (record.ExpiresMicros <= nowMicros)
            {
                Detach(record);
                _byId.Remove(record.SessionId);
                ended.Add(new SessionEndInfo(record.SessionId, record.Tag, Expired: true));
            }
            else if (record.ExpiresMicros < earliest)
            {
                earliest = record.ExpiresMicros;
            }
        }

        EarliestExpiry = earliest;
    }

    private void Detach(SessionRecord record)
    {
        int index = record.DisconnectedIndex;
        if (index < 0)
        {
            return;
        }

        int last = _disconnected.Count - 1;
        SessionRecord moved = _disconnected[last];
        _disconnected[index] = moved;
        moved.DisconnectedIndex = index;
        _disconnected.RemoveAt(last);
        record.DisconnectedIndex = -1;
    }

    private static ulong NewId()
    {
        Span<byte> bytes = stackalloc byte[8];
        RandomNumberGenerator.Fill(bytes);
        return BinaryPrimitives.ReadUInt64LittleEndian(bytes);
    }
}
