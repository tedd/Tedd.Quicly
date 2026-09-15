using System.Buffers;

namespace Tedd.Quicly.Http.Tls;

/// <summary>
/// Incremental ClientHello reassembly for the TLS peek. Each <see cref="Read"/> is given all raw bytes received so
/// far (the same prefix every time) and only walks the records completed since the previous call, so the total cost
/// is linear in the bytes received however the client fragments its hello. The pooled reassembly buffer starts at
/// the size of the first bytes and grows only as handshake payload arrives, bounded by
/// <see cref="ClientHelloParser.MaxClientHelloLength"/>.
/// </summary>
/// <remarks>A mutable struct: keep it in one local or field, never copy it, and call <see cref="Dispose"/> exactly once (not through <c>using</c>, which would operate on a copy).</remarks>
internal struct ClientHelloReader
{
    private const int MinimumBuffer = 256;

    private byte[]? _assembly;
    private ClientHelloAssemblyState _state;

    /// <summary>
    /// Continues reassembly over <paramref name="raw"/>. On <see cref="ClientHelloAssembleStatus.Complete"/>,
    /// <paramref name="parsed"/> tells whether the extensions parsed and <paramref name="hello"/> holds them.
    /// </summary>
    public ClientHelloAssembleStatus Read(ReadOnlySpan<byte> raw, out ClientHelloInfo hello, out bool parsed)
    {
        hello = default;
        parsed = false;
        const int max = ClientHelloParser.MaxClientHelloLength;
        _assembly ??= ArrayPool<byte>.Shared.Rent(Math.Clamp(raw.Length, MinimumBuffer, max));
        while (true)
        {
            int capacity = Math.Min(_assembly.Length, max);
            var status = ClientHelloParser.TryAssemble(raw, _assembly.AsSpan(0, capacity), ref _state, out int length, out _);
            if (status == ClientHelloAssembleStatus.TooLarge && capacity < max)
            {
                // Not too large yet, only larger than what has been buffered so far: grow and resume.
                int wanted = Math.Min(max, Math.Max(capacity * 2, _state.Needed));
                var larger = ArrayPool<byte>.Shared.Rent(wanted);
                _assembly.AsSpan(0, _state.Assembled).CopyTo(larger);
                ArrayPool<byte>.Shared.Return(_assembly);
                _assembly = larger;
                continue;
            }
            if (status == ClientHelloAssembleStatus.Complete)
                parsed = ClientHelloParser.TryParse(_assembly.AsSpan(0, length), out hello);
            return status;
        }
    }

    /// <summary>Returns the reassembly buffer to the pool.</summary>
    public void Dispose()
    {
        var buffer = _assembly;
        _assembly = null;
        if (buffer is not null)
            ArrayPool<byte>.Shared.Return(buffer);
    }
}
