namespace Tedd.Quicly.Replication;

/// <summary>
/// Capacity-bounded allocator of <see cref="EntityId"/>s with generation bumping on free and an optional reuse
/// delay, so an index is not handed out again while stale packets for its previous holder may still be in flight.
/// </summary>
/// <remarks>
/// <para>
/// All storage is allocated in the constructor; <see cref="TryAllocate"/>, <see cref="Free"/> and
/// <see cref="IsAlive"/> are O(1) and allocation-free.
/// </para>
/// <para>
/// Freed indices enter a FIFO queue stamped with <c>tick + ReuseDelayTicks</c>. Allocation prefers the oldest
/// freed index once its delay has passed (keeping the used index range dense), otherwise it takes a never-used
/// index, otherwise it fails. Ticks passed to <see cref="Free"/> are expected to be non-decreasing; with
/// decreasing ticks the allocator stays correct but may skip a reusable index behind a not-yet-ready one.
/// </para>
/// <para>
/// Generations wrap inside the wire mask (<see cref="EntityId.GetGenerationMask"/>) and skip 0 when at least one
/// generation bit is configured, so <see langword="default"/>(<see cref="EntityId"/>) is never alive. An index
/// returns to an old generation after 2^bits − 1 reuses; choose <c>generationBits</c> and the reuse delay so that
/// cannot happen within the lifetime of a stale packet.
/// </para>
/// <para>Not thread-safe: owned by one thread (the simulation).</para>
/// </remarks>
public sealed class EntityIdAllocator
{
    private readonly uint[] _generations;
    private readonly bool[] _alive;
    private readonly uint[] _freeIndex;
    private readonly long[] _freeReadyTick;
    private readonly uint _mask;
    private int _freeHead;
    private int _freeCount;
    private int _highWater;
    private int _count;

    /// <summary>Creates an allocator.</summary>
    /// <param name="capacity">Maximum number of simultaneously alive entities (indices 0..capacity−1); at least 1.</param>
    /// <param name="generationBits">Generation bits carried on the wire, 0..<see cref="EntityId.MaxGenerationBits"/>.</param>
    /// <param name="reuseDelayTicks">Ticks a freed index waits before it can be reused; 0 = immediately.</param>
    /// <exception cref="ArgumentOutOfRangeException">An argument is out of range.</exception>
    public EntityIdAllocator(int capacity, int generationBits = EntityId.DefaultGenerationBits, long reuseDelayTicks = 0)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(capacity, 1);
        ArgumentOutOfRangeException.ThrowIfNegative(reuseDelayTicks);
        _mask = EntityId.GetGenerationMask(generationBits);
        GenerationBits = generationBits;
        ReuseDelayTicks = reuseDelayTicks;
        _generations = new uint[capacity];
        _alive = new bool[capacity];
        _freeIndex = new uint[capacity];
        _freeReadyTick = new long[capacity];
        uint first = _mask == 0 ? 0u : 1u;
        _generations.AsSpan().Fill(first);
    }

    /// <summary>Maximum number of simultaneously alive entities.</summary>
    public int Capacity => _generations.Length;

    /// <summary>Number of currently alive entities.</summary>
    public int Count => _count;

    /// <summary>Number of indices ever handed out (all alive indices are below this value).</summary>
    public int HighWaterMark => _highWater;

    /// <summary>Generation bits carried on the wire.</summary>
    public int GenerationBits { get; }

    /// <summary>Ticks a freed index waits before reuse.</summary>
    public long ReuseDelayTicks { get; }

    /// <summary>Allocates an id.</summary>
    /// <param name="tick">The current simulation tick (compared against the reuse delay of freed indices).</param>
    /// <param name="id">The allocated id, or <see langword="default"/> on failure.</param>
    /// <returns><see langword="false"/> when every index is alive or still waiting out its reuse delay.</returns>
    public bool TryAllocate(long tick, out EntityId id)
    {
        uint index;
        if (_freeCount > 0 && _freeReadyTick[_freeHead] <= tick)
        {
            index = _freeIndex[_freeHead];
            _freeHead = _freeHead + 1 == _freeIndex.Length ? 0 : _freeHead + 1;
            _freeCount--;
        }
        else if (_highWater < _generations.Length)
        {
            index = (uint)_highWater++;
        }
        else
        {
            id = default;
            return false;
        }

        _alive[index] = true;
        _count++;
        id = new EntityId(index, _generations[index]);
        return true;
    }

    /// <summary>
    /// Frees an alive id: its generation is bumped (so the old id is no longer alive) and its index becomes
    /// reusable at <paramref name="tick"/> + <see cref="ReuseDelayTicks"/>.
    /// </summary>
    /// <param name="id">The id to free.</param>
    /// <param name="tick">The current simulation tick.</param>
    /// <returns><see langword="false"/> when <paramref name="id"/> is not alive (double free or stale id); nothing changes.</returns>
    public bool Free(EntityId id, long tick)
    {
        if (!IsAlive(id))
        {
            return false;
        }

        uint index = id.Index;
        _alive[index] = false;
        _count--;
        uint next = (_generations[index] + 1) & _mask;
        next |= (next == 0 && _mask != 0) ? 1u : 0u;
        _generations[index] = next;

        int tail = _freeHead + _freeCount;
        if (tail >= _freeIndex.Length)
        {
            tail -= _freeIndex.Length;
        }

        _freeIndex[tail] = index;
        _freeReadyTick[tail] = tick + ReuseDelayTicks;
        _freeCount++;
        return true;
    }

    /// <summary>Returns <see langword="true"/> when <paramref name="id"/> is currently allocated with exactly this generation.</summary>
    /// <param name="id">The id to test; ids decoded from the wire compare correctly because generations stay inside the wire mask.</param>
    public bool IsAlive(EntityId id)
    {
        uint index = id.Index;
        return index < (uint)_highWater && _alive[index] && _generations[index] == id.Generation;
    }

    /// <summary>Frees every id and resets every index to a never-used state (generations are kept, so old ids stay dead).</summary>
    public void Clear()
    {
        for (int i = 0; i < _highWater; i++)
        {
            if (_alive[i])
            {
                _alive[i] = false;
                uint next = (_generations[i] + 1) & _mask;
                next |= (next == 0 && _mask != 0) ? 1u : 0u;
                _generations[i] = next;
            }
        }

        // Every used index goes back to the free queue, immediately reusable (no packets can refer to them once the
        // application has decided to clear the world, e.g. on a new epoch).
        _freeHead = 0;
        _freeCount = _highWater;
        for (int i = 0; i < _highWater; i++)
        {
            _freeIndex[i] = (uint)i;
            _freeReadyTick[i] = long.MinValue;
        }

        _count = 0;
    }
}
