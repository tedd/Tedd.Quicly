using Tedd.Quicly.Testing.Conformance;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Transport.MsQuic.Tests.WebTransport;

/// <summary>
/// The shared transport conformance suite against the WebTransport-over-HTTP/3 carrier. The carrier presents the same
/// <see cref="Core.Transport.ITransport"/> contract as the raw carrier, so every scenario has to pass unchanged — that
/// is what makes the two carriers interchangeable under Core.
/// </summary>
public class WebTransportConformanceTests
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
        using var harness = new WebTransportSimulatedHarness();
        TransportConformance.Run(name, harness);
        Assert.Equal(0, harness.Network.InvariantViolations);
    }

    [Theory]
    [MemberData(nameof(Scenarios))]
    public void Scenario_on_a_jittery_link_with_stream_packet_loss(string name)
    {
        using var harness = new WebTransportSimulatedHarness(new LinkOptions { DelayMicros = 10_000, JitterMicros = 3_000, StreamLossPercent = 10 }, seed: 5);
        TransportConformance.Run(name, harness);
        Assert.Equal(0, harness.Network.InvariantViolations);
    }
}
