using System.Text;

namespace Tedd.Quicly.Testing.Simulation;

/// <summary>The path between two simulated transports: options snapshot, current MTU, and the scheduled timeline.</summary>
internal sealed class SimulatedLink
{
    public readonly SimulatedNetwork Network;
    public readonly LinkOptions Options;
    public readonly long CreatedMicros;
    public readonly byte[] Alpn;
    public SimulatedTransport? A;
    public SimulatedTransport? B;
    public int MaxPayload;

    public SimulatedLink(SimulatedNetwork network, LinkOptions options, string alpn)
    {
        Network = network;
        Options = options;
        CreatedMicros = network.NowMicros;
        MaxPayload = options.MaxDatagramPayload;
        Alpn = Encoding.ASCII.GetBytes(alpn);
    }

    public void ScheduleTimeline()
    {
        foreach (MtuChange change in Options.MtuChanges)
        {
            SimEvent e = new() { Kind = SimEventKind.MtuChange, Due = CreatedMicros + change.AtMicros, Obj = this, I0 = change.MaxDatagramPayload };
            Network.Schedule(ref e);
        }
        if (Options.DisconnectAtMicros is long at)
        {
            SimEvent e = new() { Kind = SimEventKind.Disconnect, Due = CreatedMicros + at, Obj = this };
            Network.Schedule(ref e);
        }
    }

    public void Dispatch(ref SimEvent e)
    {
        if (e.Kind == SimEventKind.MtuChange)
        {
            MaxPayload = e.I0;
            A?.OnMtuChanged();
            B?.OnMtuChanged();
        }
        else
        {
            A?.OnDisconnected();
            B?.OnDisconnected();
        }
    }
}
