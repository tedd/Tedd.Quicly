namespace Tedd.Quicly.Transport.MsQuic.Tests;

public class SmokeTests
{
    [Fact]
    public void Runtime_Is_Expected() => Assert.True(Environment.Version.Major >= 10);
}
