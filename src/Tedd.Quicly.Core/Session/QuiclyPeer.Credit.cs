using System.Runtime.CompilerServices;
using Tedd.Quicly.Core.State;

namespace Tedd.Quicly.Core.Session;

// Game thread: the receive credit of the reliable stream channels (ReceiveCredit) as Poll, Drain and the handler
// registration see it — where a message's credit comes back, when a channel's limit applies, and when the streams a
// channel held back are resumed (docs/design/session-layer.md §4.4).
public sealed unsafe partial class QuiclyPeer
{
    /// <summary>Per dense channel: the channel is counted by <see cref="ReceiveCredit"/> (its engine enabled it; fixed after construction).</summary>
    private readonly bool[] _credited;

    /// <summary>A channel has a handler and still the limit of a channel nobody reads: its backlog was not dispatched yet.</summary>
    private bool _creditSettleDue;

    /// <summary>
    /// Gives back the credit of a message that leaves the receive ring, the held slot or a drain queue for the application:
    /// a handler is about to see it, or <see cref="Drain"/> is about to hand it out. From here on the application owns it,
    /// so it no longer counts against what its channel may have waiting — whether or not the payload is released soon.
    /// Called before the message is decoded, because the transport thread counted the lease it arrived in.
    /// </summary>
    /// <param name="index">Dense index of the message's channel.</param>
    /// <param name="entry">The message.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ReturnCredit(int index, in ReceiveEntry entry)
    {
        if (_credited[index])
        {
            _core.Credit.NoteReturned(index, entry.Lease.Length, entry.CreditShared != 0);
        }
    }

    /// <summary>
    /// Resumes the streams that were held back for a channel's credit and whose channel got some back since (the end of
    /// <see cref="Drain"/>, every <see cref="Poll"/>, a handler registration). A stream whose channel is still out of
    /// credit stays where it is: unlike the ring's and the budget's back-pressure, this one is not retried by every Poll.
    /// </summary>
    private void ResumeCreditPended() => _core.Credit.Resume(_transport);

    /// <summary>
    /// Lifts the limit of a channel that has a handler and nothing left in its drain queue: its messages now leave the
    /// receive ring at every <see cref="Poll"/>, so the ring's own back-pressure is all it needs. Called where that can
    /// become true — a handler is registered, a Poll has dispatched a channel's queue, a <see cref="Drain"/> has emptied it.
    /// A handler registered over a backlog therefore keeps the limit until that backlog is gone: registering and removing
    /// a handler again and again without a Poll in between cannot add a ring's worth of messages to the queue each time.
    /// </summary>
    /// <param name="index">Dense channel index.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void SettleCreditLimit(int index)
    {
        if (_credited[index] && _handlers[index] is not null)
        {
            LiftCreditLimit(index);
        }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void LiftCreditLimit(int index)
    {
        ReceiveCredit credit = _core.Credit;
        if (!credit.IsLimited(index))
        {
            return;
        }

        if (_queues.Count(index) == 0)
        {
            // The streams the channel held back are resumed by the ResumeCreditPended that follows every call site.
            credit.SetState(index, CreditState.Handled);
        }
        else
        {
            // Not yet: every Poll looks again until the backlog is gone (SettleDueCreditLimits), however it goes — also
            // when the handler throws on its last message and the Poll ends before it gets here again.
            _creditSettleDue = true;
        }
    }

    /// <summary>
    /// The end of a Poll's dispatch: lifts the limit of every channel whose handler has seen its backlog by now
    /// (<see cref="SettleCreditLimit"/>). One compare while no handler was registered over a backlog.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void SettleDueCreditLimits()
    {
        if (_creditSettleDue)
        {
            SettleDueCreditLimitsCore();
        }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void SettleDueCreditLimitsCore()
    {
        _creditSettleDue = false;
        bool[] credited = _credited;
        for (int index = 0; index < credited.Length; index++)
        {
            // LiftCreditLimit asks for another look when a channel's backlog is still there.
            SettleCreditLimit(index);
        }
    }

    /// <summary>
    /// A channel lost its handler: from now on its messages wait for <see cref="Drain"/>, and until a Drain has shown that
    /// the application reads it that way, within the share of a channel nobody reads.
    /// </summary>
    /// <param name="index">Dense channel index.</param>
    private void LimitCredit(int index)
    {
        if (_credited[index])
        {
            _core.Credit.SetState(index, CreditState.Unread);
        }
    }

    /// <summary>
    /// The end of a <see cref="Drain"/> of a channel: with a handler, see <see cref="SettleCreditLimit"/>. Without one, a
    /// Drain that left nothing queued shows that the application reads the channel this way, and from here on the ring
    /// and the receive budget are its limits, as they are for a handler (<see cref="CreditState.Drained"/>) — until a
    /// <see cref="Poll"/> finds that a whole pass went by without it (<see cref="DemoteUndrainedChannels"/>).
    /// </summary>
    /// <param name="index">Dense channel index.</param>
    /// <param name="channel">The channel's id.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void SettleCreditAfterDrain(int index, ushort channel)
    {
        if (_credited[index])
        {
            SettleCreditAfterDrainCore(index, channel);
        }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void SettleCreditAfterDrainCore(int index, ushort channel)
    {
        if (_queues.Count(index) != 0 || (_hasHeld && _held.Channel == channel))
        {
            // A channel with a handler is looked at again by the next Poll, which dispatches what is left.
            _creditSettleDue |= _handlers[index] is not null && _core.Credit.IsLimited(index);
            return;
        }

        // The streams the channel held back are resumed by the ResumeCreditPended that follows.
        _core.Credit.SetState(index, _handlers[index] is not null ? CreditState.Handled : CreditState.Drained);
    }

    /// <summary>
    /// The start of a pass (<see cref="Poll"/>, after <see cref="ReceiveQueues.BeginPass"/>): a channel the application
    /// was draining and that still has messages queued which no Drain took during the whole pass before is a channel
    /// nobody reads, from here on (<see cref="CreditState.Unread"/>). It keeps what it has, and accepts nothing more until
    /// the application has taken that down to its share. Costs one compare while no channel is read with Drain.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void DemoteUndrainedChannels()
    {
        if (_core.Credit.DrainedChannels != 0)
        {
            DemoteUndrainedChannelsCore();
        }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void DemoteUndrainedChannelsCore()
    {
        ReceiveCredit credit = _core.Credit;
        ReceiveQueues queues = _queues;
        bool[] credited = _credited;
        for (int index = 0; index < credited.Length; index++)
        {
            if (credited[index] && credit.State(index) == CreditState.Drained && queues.LeftUndrained(index))
            {
                credit.SetState(index, CreditState.Unread);
            }
        }
    }

    /// <summary>
    /// Puts every channel's limit back in step with its handler after <see cref="PeerCore.ResetForReconnect"/> zeroed the
    /// counts (the queues were emptied before it): handlers survive a reconnect, a backlog does not. A channel without a
    /// handler keeps its state: the application reads it, or does not, as it did on the lost connection.
    /// </summary>
    private void ResetCreditLimitsForReconnect()
    {
        ReceiveCredit credit = _core.Credit;
        for (int index = 0; index < _handlers.Length; index++)
        {
            if (_credited[index] && _handlers[index] is not null)
            {
                credit.SetState(index, CreditState.Handled);
            }
        }
    }
}
