namespace Tedd.Quicly.Server;

/// <summary>
/// Events found on other threads (the listener's callbacks, the certificate binder) waiting for the game thread, which raises
/// them from <see cref="QuiclyServer.PollAll"/>. Bounded: beyond the capacity an event is dropped and counted.
/// </summary>
/// <typeparam name="T">The event's payload.</typeparam>
internal sealed class EventQueue<T>(int capacity)
{
    private readonly Lock _gate = new();
    private List<T> _items = [];
    private List<T> _spare = [];
    private int _count;
    private long _dropped;
    private bool _draining;

    /// <summary>Whether events wait (any thread; a hint).</summary>
    public bool HasItems => Volatile.Read(ref _count) != 0;

    /// <summary>Events dropped because the queue was full.</summary>
    public long Dropped => Interlocked.Read(ref _dropped);

    /// <summary>Queues <paramref name="item"/> (any thread).</summary>
    public void Enqueue(in T item)
    {
        lock (_gate)
        {
            if (_items.Count >= capacity)
            {
                Interlocked.Increment(ref _dropped);
                return;
            }

            _items.Add(item);
            Volatile.Write(ref _count, _items.Count);
        }
    }

    /// <summary>
    /// Hands every queued event to <paramref name="raise"/>, oldest first (game thread; <paramref name="raise"/> must not
    /// throw). A call from inside <paramref name="raise"/> returns at once: the rest wait for the next call.
    /// </summary>
    public void Drain(Action<T> raise)
    {
        if (_draining)
        {
            return;
        }

        List<T> batch;
        lock (_gate)
        {
            batch = _items;
            _items = _spare;
            _spare = batch;
            Volatile.Write(ref _count, 0);
        }

        _draining = true;
        try
        {
            foreach (T item in batch)
            {
                raise(item);
            }
        }
        finally
        {
            batch.Clear();
            _draining = false;
        }
    }
}
