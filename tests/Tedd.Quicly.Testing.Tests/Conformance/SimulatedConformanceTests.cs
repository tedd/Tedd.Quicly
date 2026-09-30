using Tedd.Quicly.Testing.Conformance;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Testing.Tests.Conformance;

/// <summary>The shared transport conformance suite against <see cref="SimulatedTransport"/>, on a clean and on a hostile link.</summary>
public class SimulatedConformanceTests
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
        using var harness = new SimulatedTransportHarness();
        TransportConformance.Run(name, harness);
        Assert.Equal(0, harness.Network.InvariantViolations);
    }

    [Theory]
    [MemberData(nameof(Scenarios))]
    public void Scenario_on_a_jittery_link_with_stream_packet_loss(string name)
    {
        using var harness = new SimulatedTransportHarness(new LinkOptions { DelayMicros = 10_000, JitterMicros = 3_000, StreamLossPercent = 10 }, seed: 5);
        TransportConformance.Run(name, harness);
        Assert.Equal(0, harness.Network.InvariantViolations);
    }

    [Fact]
    public void Unknown_scenario_is_rejected()
    {
        using var harness = new SimulatedTransportHarness();
        Assert.Throws<ArgumentException>(() => TransportConformance.Run("NoSuchScenario", harness));
        Assert.Equal(28, TransportConformance.ScenarioNames.Count);
    }
}
