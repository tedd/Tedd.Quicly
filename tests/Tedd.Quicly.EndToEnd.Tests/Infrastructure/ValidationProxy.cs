using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;

namespace Tedd.Quicly.EndToEnd.Tests.Infrastructure;

/// <summary>
/// A transparent loopback TCP relay between the fake CA's tls-alpn-01 validation client and the provisioner's TLS endpoint.
/// When the CA connects, the challenge certificate is known to be published (the CA validates only after the client has
/// answered the challenge, and the client answers only after publishing), so <c>beforeForwarding</c> runs its probes at
/// exactly that moment. The CA's connection is then relayed unchanged, so the CA still validates what the endpoint serves.
/// </summary>
internal sealed class ValidationProxy : IAsyncDisposable
{
    private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
    private readonly Func<IPEndPoint?> _target;
    private readonly Func<CancellationToken, Task> _beforeForwarding;
    private readonly CancellationTokenSource _stop = new();
    private readonly ConcurrentQueue<Exception> _errors = new();
    private readonly Lock _lock = new();
    private readonly List<Task> _relays = [];
    private readonly Task _acceptLoop;
    private int _connections;

    public ValidationProxy(Func<IPEndPoint?> target, Func<CancellationToken, Task> beforeForwarding)
    {
        _target = target;
        _beforeForwarding = beforeForwarding;
        _listener.Start();
        Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
        _acceptLoop = Task.Run(AcceptLoopAsync);
    }

    /// <summary>The loopback port the CA is told to validate against.</summary>
    public int Port { get; }

    /// <summary>Connections relayed so far (one per tls-alpn-01 validation).</summary>
    public int Connections => Volatile.Read(ref _connections);

    /// <summary>Exceptions thrown by <c>beforeForwarding</c> or by the relay itself.</summary>
    public Exception[] Errors => [.. _errors];

    public async ValueTask DisposeAsync()
    {
        await _stop.CancelAsync().ConfigureAwait(false);
        _listener.Stop();
        try
        {
            await _acceptLoop.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }

        Task[] relays;
        lock (_lock)
        {
            relays = [.. _relays];
        }

        try
        {
            await Task.WhenAll(relays).WaitAsync(E2eTimeouts.Step).ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            // A relay stuck on a socket that never closes; its sockets are abandoned with the test.
        }

        _stop.Dispose();
    }

    private async Task AcceptLoopAsync()
    {
        while (!_stop.IsCancellationRequested)
        {
            Socket client;
            try
            {
                client = await _listener.AcceptSocketAsync(_stop.Token).ConfigureAwait(false);
            }
            catch (Exception) when (_stop.IsCancellationRequested)
            {
                return;
            }
            catch (SocketException e)
            {
                _errors.Enqueue(e);
                return;
            }

            Interlocked.Increment(ref _connections);
            lock (_lock)
            {
                _relays.Add(Task.Run(() => RelayAsync(client)));
            }
        }
    }

    private async Task RelayAsync(Socket client)
    {
        using (client)
        {
            try
            {
                await _beforeForwarding(_stop.Token).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                // Recorded for the test; the CA still gets its relay, so its verdict reflects the endpoint, not the probe.
                _errors.Enqueue(e);
            }

            if (_target() is not { } target)
            {
                _errors.Enqueue(new InvalidOperationException("The validation proxy has no end point to relay to."));
                return;
            }

            using Socket upstream = new(target.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            try
            {
                await upstream.ConnectAsync(target, _stop.Token).ConfigureAwait(false);
                await Task.WhenAll(PumpAsync(client, upstream), PumpAsync(upstream, client)).ConfigureAwait(false);
            }
            catch (Exception e) when (e is SocketException or IOException or OperationCanceledException)
            {
                // One side went away; the CA reports what it saw.
            }
        }
    }

    /// <summary>Copies one direction until end of stream, then half-closes the other side; a reset aborts both directions.</summary>
    private async Task PumpAsync(Socket from, Socket to)
    {
        byte[] buffer = new byte[16 * 1024];
        try
        {
            using NetworkStream source = new(from, ownsSocket: false);
            using NetworkStream sink = new(to, ownsSocket: false);
            int read;
            while ((read = await source.ReadAsync(buffer, _stop.Token).ConfigureAwait(false)) > 0)
            {
                await sink.WriteAsync(buffer.AsMemory(0, read), _stop.Token).ConfigureAwait(false);
            }

            to.Shutdown(SocketShutdown.Send);
        }
        catch (Exception e) when (e is SocketException or IOException or ObjectDisposedException or OperationCanceledException)
        {
            try
            {
                to.Shutdown(SocketShutdown.Both);
            }
            catch (Exception inner) when (inner is SocketException or ObjectDisposedException)
            {
            }
        }
    }
}
