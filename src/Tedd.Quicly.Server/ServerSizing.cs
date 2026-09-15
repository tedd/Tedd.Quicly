using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Session;

namespace Tedd.Quicly.Server;

/// <summary>
/// Table and budget sizes a <see cref="QuiclyServer"/> derived from <see cref="ServerOptions.ExpectedPeers"/> and
/// <see cref="ServerOptions.MaxPeers"/> (ARCHITECTURE.md §9). Read the effective values from <see cref="QuiclyServer.Sizing"/>.
/// </summary>
/// <remarks>
/// <para>Per-peer values that still hold their <see cref="PeerOptions"/> default are scaled by tier; values the
/// application changed in <see cref="ServerOptions.PeerOptions"/> are kept.</para>
/// <list type="table">
/// <listheader><term>ExpectedPeers</term><description>send table / receive ring / segment arena / send and receive budget</description></listheader>
/// <item><term>up to 64</term><description>1 024 / 4 096 / 1 024 / 256 KiB (the PeerOptions defaults)</description></item>
/// <item><term>up to 256</term><description>512 / 2 048 / 512 / 256 KiB</description></item>
/// <item><term>up to 1 024</term><description>256 / 1 024 / 256 / 128 KiB</description></item>
/// <item><term>more</term><description>128 / 512 / 128 / 64 KiB</description></item>
/// </list>
/// <para>Unless the template names an <see cref="PeerOptions.Allocator"/>, every peer shares one buffer pool: built from
/// the template's <see cref="PeerOptions.AllocatorOptions"/> when set, otherwise from the default size classes (16 MiB)
/// scaled to <see cref="PoolBytesPerExpectedPeer"/> per expected peer (at least 16 MiB, at most 1 GiB). Budgets bound what
/// one peer may hold; the pool is what all peers share. The tiers are provisional until docs/benchmarks/memory.md publishes
/// measured per-peer usage.</para>
/// </remarks>
/// <param name="SlotCapacity">Peer slots (and <see cref="PeerSet"/> capacity): <see cref="ServerOptions.MaxPeers"/> + <see cref="ServerAdmissionOptions.MaxUnadmittedConnections"/>.</param>
/// <param name="SendTableCapacity">Per-peer <see cref="PeerOptions.SendTableCapacity"/>.</param>
/// <param name="ReceiveRingCapacity">Per-peer <see cref="PeerOptions.ReceiveRingCapacity"/>.</param>
/// <param name="SegmentArenaCapacity">Per-peer <see cref="PeerOptions.SegmentArenaCapacity"/>.</param>
/// <param name="SendBudgetBytes">Per-peer <see cref="PeerOptions.SendBudgetBytes"/>.</param>
/// <param name="ReceiveBudgetBytes">Per-peer <see cref="PeerOptions.ReceiveBudgetBytes"/>.</param>
/// <param name="SharedPoolBytes">Bytes reserved by the shared pool the server created; 0 when the template supplied the allocator.</param>
public readonly record struct ServerSizing(
    int SlotCapacity,
    int SendTableCapacity,
    int ReceiveRingCapacity,
    int SegmentArenaCapacity,
    int SendBudgetBytes,
    int ReceiveBudgetBytes,
    long SharedPoolBytes)
{
    /// <summary>Shared pool bytes per expected peer when the default size classes are scaled.</summary>
    public const int PoolBytesPerExpectedPeer = 128 * 1024;

    private const long DefaultPoolBytes = 16L * 1024 * 1024;
    private const int MaxPoolFactor = 64;

    /// <summary>Computes the sizes and the options of the shared pool to create (null when the template supplies an allocator).</summary>
    internal static ServerSizing Compute(ServerOptions options, out SlabAllocatorOptions? poolOptions)
    {
        PeerOptions template = options.PeerOptions;
        PeerOptions defaults = new();
        int expected = options.ExpectedPeers;
        int tier = expected <= 64 ? 0 : expected <= 256 ? 1 : expected <= 1024 ? 2 : 3;
        int budget = tier switch
        {
            0 or 1 => 256 * 1024,
            2 => 128 * 1024,
            _ => 64 * 1024,
        };

        int sendTable = Pick(template.SendTableCapacity, defaults.SendTableCapacity, 1024 >> tier);
        int ring = Pick(template.ReceiveRingCapacity, defaults.ReceiveRingCapacity, 4096 >> tier);
        int arena = Pick(template.SegmentArenaCapacity, defaults.SegmentArenaCapacity, 1024 >> tier);
        int sendBudget = Pick(template.SendBudgetBytes, defaults.SendBudgetBytes, budget);
        int receiveBudget = Pick(template.ReceiveBudgetBytes, defaults.ReceiveBudgetBytes, budget);

        poolOptions = null;
        long poolBytes = 0;
        if (template.Allocator is null)
        {
            if (template.AllocatorOptions is { } custom)
            {
                poolOptions = custom;
                poolBytes = SumBytes(custom.SizeClasses);
            }
            else
            {
                long wanted = (long)expected * PoolBytesPerExpectedPeer;
                int factor = (int)Math.Clamp((wanted + DefaultPoolBytes - 1) / DefaultPoolBytes, 1, MaxPoolFactor);
                SizeClassDefinition[] classes = SlabAllocatorOptions.CreateDefaultSizeClasses();
                for (int i = 0; i < classes.Length; i++)
                {
                    classes[i] = new SizeClassDefinition(classes[i].BlockSize, classes[i].BlockCount * factor);
                }

                poolOptions = new SlabAllocatorOptions { SizeClasses = classes };
                poolBytes = SumBytes(classes);
            }
        }

        return new ServerSizing(options.MaxPeers + options.Admission.MaxUnadmittedConnections, sendTable, ring, arena, sendBudget, receiveBudget, poolBytes);
    }

    private static int Pick(int configured, int defaultValue, int scaled) => configured != defaultValue ? configured : scaled;

    private static long SumBytes(SizeClassDefinition[]? classes)
    {
        long total = 0;
        if (classes is not null)
        {
            foreach (SizeClassDefinition definition in classes)
            {
                total += definition.SlabBytes;
            }
        }

        return total;
    }
}
