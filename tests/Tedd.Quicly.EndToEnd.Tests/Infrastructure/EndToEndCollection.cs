namespace Tedd.Quicly.EndToEnd.Tests.Infrastructure;

/// <summary>
/// Every end-to-end test class joins this collection, so the suite runs one test at a time inside each test process. It
/// shares the machine with other test runs, and every test drives real sockets, TLS and MsQuic worker threads; running
/// serially keeps one test's load from stretching another test's handshakes. (The two target frameworks still run as two
/// processes side by side: every endpoint binds loopback port 0 and every file lives in a folder of its own.)
/// </summary>
[CollectionDefinition(Name)]
public sealed class EndToEndCollection
{
    public const string Name = "EndToEnd";
}
