using Tedd.Quicly.Testing.Conformance;

namespace Tedd.Quicly.Transport.MsQuic.Tests.Transport;

/// <summary>The shared transport conformance suite against <see cref="MsQuicTransport"/> over loopback.</summary>
[Collection(MsQuicCollection.Name)]
public class MsQuicConformanceTests
{
    public static TheoryData<string> Scenarios()
    {
        var data = new TheoryData<string>();
        foreach (string name in TransportConformance.ScenarioNames) data.Add(name);
        return data;
    }

    [Theory]
    [MemberData(nameof(Scenarios))]
    public void Scenario(string name)
    {
        var harness = new MsQuicTransportHarness();
        try
        {
            TransportConformance.Run(name, harness);
        }
        finally
        {
            harness.Dispose();
        }
        Assert.Null(harness.CleanupError);
        Assert.Equal(0, harness.SinkExceptionTotal);
    }
}
