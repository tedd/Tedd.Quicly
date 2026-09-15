using System.Runtime.InteropServices;

namespace Tedd.Quicly.Transport.MsQuic;

/// <summary>
/// Tracks whether the current thread is inside an MsQuic callback (ADR 0008 §7). Every
/// <c>[UnmanagedCallersOnly]</c> entry point in this assembly brackets its body with <see cref="Enter"/> /
/// <see cref="Exit"/>; <see cref="MsQuicConnection.Close"/> and <see cref="MsQuicStream.Close"/> refuse to run
/// while it is set, because closing a handle from its own callback thread is the classic MsQuic deadlock /
/// use-after-free trap. Application code can consult <see cref="IsInsideCallback"/> for its own assertions.
/// </summary>
/// <remarks>
/// <para><b>Stricter than MsQuic.</b> MsQuic itself only forbids closing a handle from that handle's own callback
/// (and lets a stream be closed inline from PEER_STREAM_STARTED). This guard refuses Close from <i>any</i> MsQuic
/// callback thread, including callbacks of unrelated connections, so the transport's shutdown path (C3) must
/// always hand Close to its owner thread rather than run it from an event.</para>
/// <para><b>Cost.</b> The guard is active in release builds too: one thread-static increment and decrement per
/// callback (ADR 0008 §7 asks for at least a debug-only flag). It has not been benchmarked separately; if an
/// interop baseline shows it is measurable, make it conditional per ADR 0007.</para>
/// <para><b>Invalid contexts.</b> A callback whose native context does not resolve to the expected wrapper
/// (null, or a handle to another object) answers <c>QUIC_STATUS_INTERNAL_ERROR</c> and is counted in
/// <see cref="InvalidContextCount"/>; it never throws out of the <c>[UnmanagedCallersOnly]</c> method.</para>
/// </remarks>
public static class MsQuicCallbackScope
{
    [ThreadStatic]
    private static int t_depth;

    /// <summary>True while the current thread is executing an MsQuic callback dispatched by this assembly.</summary>
    public static bool IsInsideCallback => t_depth > 0;

    /// <summary>
    /// When true an exception escaping an event handler terminates the process immediately (strict mode,
    /// ADR 0008 §8) instead of being recorded on the owning object and turned into a shutdown. Off by default.
    /// </summary>
    public static bool FailFastOnCallbackException { get; set; }

    private static long s_invalidContexts;

    /// <summary>Number of callbacks (process wide) whose native context did not resolve to a live wrapper.</summary>
    public static long InvalidContextCount => Interlocked.Read(ref s_invalidContexts);

    /// <summary>
    /// Resolves an MsQuic callback context to its wrapper. Returns null (and counts it) for a null context, a handle
    /// that has been freed or a target of another type; never throws.
    /// </summary>
    internal static unsafe T? ResolveContext<T>(void* context) where T : class
    {
        if (context != null)
        {
            try
            {
                if (GCHandle.FromIntPtr((nint)context).Target is T target) return target;
            }
            catch (InvalidOperationException)
            {
                // A freed / never-allocated handle: fall through and count it.
            }
        }
        Interlocked.Increment(ref s_invalidContexts);
        return null;
    }

    internal static void Enter() => t_depth++;

    internal static void Exit() => t_depth--;

    /// <summary>Throws <see cref="InvalidOperationException"/> when called from a callback thread.</summary>
    internal static void ThrowIfInsideCallback(string operation)
    {
        if (IsInsideCallback)
        {
            throw new InvalidOperationException($"{operation} must not be called from an MsQuic callback; defer it to another thread (ADR 0008 §7).");
        }
    }

    /// <summary>Strict-mode hook: fails fast when enabled, otherwise returns so the caller can record and poison.</summary>
    internal static void OnEscapedException(Exception exception)
    {
        if (FailFastOnCallbackException)
        {
            Environment.FailFast("An exception escaped an MsQuic callback handler.", exception);
        }
    }
}
