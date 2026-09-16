using Tedd.Quicly.Core.Session;

namespace Tedd.Quicly.Client;

/// <summary>
/// Wakes <see cref="QuiclyClient.ConnectAsync"/> when its peer publishes game-thread work, so the connect polls the peer
/// promptly instead of on a fixed interval. It is the peer's <see cref="PeerOptions.WorkSignal"/>: the peer raises it once
/// per <see cref="QuiclyPeer.Poll"/> (an edge) from whichever thread produced the work — a transport thread for the
/// handshake, received traffic and completions, the client's own thread for work an application call created.
/// </summary>
internal sealed class WorkSignal : IPeerWorkSignal, IDisposable
{
    private readonly SemaphoreSlim _semaphore = new(0);
    private int _pending;

    /// <summary>Whether the last <see cref="WaitAsync"/> ended because of a <see cref="Set"/> rather than its timeout (tests).</summary>
    internal bool LastWaitSignaled { get; private set; }

    /// <summary>The peer published work (any thread): never blocks, never allocates, never re-enters the peer.</summary>
    /// <param name="peer">The peer with work waiting (the client drives exactly one).</param>
    public void OnWork(QuiclyPeer peer) => Set();

    /// <summary>Releases a waiter (or the next one) unless a release is already outstanding; never blocks.</summary>
    public void Set()
    {
        if (Interlocked.Exchange(ref _pending, 1) != 0)
        {
            return;
        }

        try
        {
            _semaphore.Release();
        }
        catch (ObjectDisposedException)
        {
            // A late callback after the client was disposed.
        }
    }

    /// <summary>Waits until <see cref="Set"/> or <paramref name="timeout"/>; the caller then polls the peer.</summary>
    /// <remarks>
    /// The pending flag is cleared when the wait ends, before the caller polls: a <see cref="Set"/> for work published during
    /// that poll finds the flag clear and releases again, so the next wait returns at once instead of sleeping its timeout.
    /// A <see cref="Set"/> between the wake-up and the clear is covered by the poll that follows (its work was published
    /// first). The clear is a full fence (<see cref="Interlocked.Exchange(ref int, int)"/>), so the poll's reads cannot move
    /// before it. At worst a stale release makes a later wait return at once.
    /// </remarks>
    public async ValueTask WaitAsync(TimeSpan timeout, CancellationToken cancellationToken)
    {
        try
        {
            LastWaitSignaled = await _semaphore.WaitAsync(timeout, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            Interlocked.Exchange(ref _pending, 0);
        }
    }

    public void Dispose() => _semaphore.Dispose();
}
