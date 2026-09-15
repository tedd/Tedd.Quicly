namespace Tedd.Quicly.Transport.MsQuic;

/// <summary>
/// Tracks whether the current thread is inside an MsQuic callback (ADR 0008 §7). Every
/// <c>[UnmanagedCallersOnly]</c> entry point in this assembly brackets its body with <see cref="Enter"/> /
/// <see cref="Exit"/>; <see cref="MsQuicConnection.Close"/> and <see cref="MsQuicStream.Close"/> refuse to run
/// while it is set, because closing a handle from its own callback thread is the classic MsQuic deadlock /
/// use-after-free trap. Application code can consult <see cref="IsInsideCallback"/> for its own assertions.
/// </summary>
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
