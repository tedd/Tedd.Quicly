using System.Buffers.Binary;
using System.Reflection;
using System.Security.Cryptography;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Time;

namespace Tedd.Quicly.Core.Tests.Control;

public class SessionTokenAuthorityTests
{
    private static byte[] Key(byte fill) => Enumerable.Repeat(fill, SessionTokenAuthority.KeyLength).ToArray();

    private static byte[] Mint(SessionTokenAuthority authority, ulong sessionId, uint epoch, long expiry)
    {
        byte[] token = new byte[SessionTokenAuthority.TokenLength];
        Assert.Equal(SessionTokenAuthority.TokenLength, authority.Mint(sessionId, epoch, expiry, token));
        return token;
    }

    [Fact]
    public void Layout_Is_Version_Id_Epoch_Expiry_Random_Mac()
    {
        VirtualClock clock = new(1000);
        byte[] key = Key(0xA5);
        using SessionTokenAuthority authority = new(key, clock);
        byte[] token = Mint(authority, 0x0102030405060708, 0x0A0B0C0D, 123_456_789);

        Assert.Equal(69, token.Length);
        Assert.Equal(1, token[0]);
        Assert.Equal(0x0102030405060708UL, BinaryPrimitives.ReadUInt64LittleEndian(token.AsSpan(1)));
        Assert.Equal(0x0A0B0C0Du, BinaryPrimitives.ReadUInt32LittleEndian(token.AsSpan(9)));
        Assert.Equal(123_456_789L, BinaryPrimitives.ReadInt64LittleEndian(token.AsSpan(13)));
        Assert.Equal(HMACSHA256.HashData(key, token.AsSpan(0, 37)), token.AsSpan(37).ToArray());

        byte[] other = Mint(authority, 0x0102030405060708, 0x0A0B0C0D, 123_456_789);
        Assert.NotEqual(token.AsSpan(21, 16).ToArray(), other.AsSpan(21, 16).ToArray());
    }

    [Fact]
    public void Valid_Token_Is_Single_Use()
    {
        VirtualClock clock = new(0);
        using SessionTokenAuthority authority = new(Key(1), clock);
        byte[] token = Mint(authority, 77, 3, 1_000_000);

        Assert.Equal(SessionTokenStatus.Valid, authority.TryInspect(token, out ulong sessionId, out uint epoch));
        Assert.Equal(77UL, sessionId);
        Assert.Equal(3u, epoch);
        Assert.Equal(SessionTokenStatus.Valid, authority.TryInspect(token, out _, out _));
        Assert.Equal(0, authority.ReplayCacheCount);

        Assert.Equal(SessionTokenStatus.Valid, authority.TryValidate(token, out sessionId, out epoch));
        Assert.Equal(77UL, sessionId);
        Assert.Equal(3u, epoch);
        Assert.Equal(1, authority.ReplayCacheCount);

        Assert.Equal(SessionTokenStatus.Replayed, authority.TryValidate(token, out sessionId, out epoch));
        Assert.Equal(0UL, sessionId);
        Assert.Equal(0u, epoch);
        Assert.Equal(SessionTokenStatus.Replayed, authority.TryInspect(token, out _, out _));
    }

    [Fact]
    public void Tampering_With_Any_Byte_Fails()
    {
        VirtualClock clock = new(0);
        using SessionTokenAuthority authority = new(Key(2), clock);
        byte[] token = Mint(authority, 5, 1, 1_000_000);
        for (int i = 0; i < token.Length; i++)
        {
            foreach (byte flip in new byte[] { 0x01, 0x80, 0xFF })
            {
                byte[] tampered = (byte[])token.Clone();
                tampered[i] ^= flip;
                SessionTokenStatus status = authority.TryValidate(tampered, out ulong sessionId, out uint epoch);
                Assert.Equal(i == 0 ? SessionTokenStatus.Malformed : SessionTokenStatus.BadSignature, status);
                Assert.Equal(0UL, sessionId);
                Assert.Equal(0u, epoch);
            }
        }

        Assert.Equal(0, authority.ReplayCacheCount);
        Assert.Equal(SessionTokenStatus.Valid, authority.TryValidate(token, out _, out _));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(56)]
    [InlineData(68)]
    [InlineData(70)]
    [InlineData(4096)]
    public void Wrong_Length_Is_Malformed(int length)
    {
        using SessionTokenAuthority authority = new(Key(3), new VirtualClock());
        byte[] token = new byte[length];
        if (length > 0)
        {
            token[0] = SessionTokenAuthority.FormatVersion;
        }

        Assert.Equal(SessionTokenStatus.Malformed, authority.TryValidate(token, out _, out _));
    }

    [Fact]
    public void Token_From_Another_Key_Has_Bad_Signature()
    {
        VirtualClock clock = new(0);
        using SessionTokenAuthority a = new(Key(4), clock);
        using SessionTokenAuthority b = new(Key(5), clock);
        Assert.Equal(SessionTokenStatus.BadSignature, b.TryValidate(Mint(a, 1, 1, 100), out _, out _));
    }

    [Fact]
    public void Expiry_Is_Exclusive()
    {
        VirtualClock clock = new(0);
        using SessionTokenAuthority authority = new(Key(6), clock);
        byte[] token = Mint(authority, 1, 1, 500);
        clock.Set(499);
        Assert.Equal(SessionTokenStatus.Valid, authority.TryInspect(token, out _, out _));
        clock.Set(500);
        Assert.Equal(SessionTokenStatus.Expired, authority.TryValidate(token, out _, out _));
        clock.Set(10_000);
        Assert.Equal(SessionTokenStatus.Expired, authority.TryValidate(token, out _, out _));
        Assert.Equal(SessionTokenStatus.Expired, authority.TryValidate(Mint(authority, 1, 1, -5), out _, out _));
    }

    [Fact]
    public void Consumed_Token_Reports_Expired_Once_Expired()
    {
        VirtualClock clock = new(0);
        using SessionTokenAuthority authority = new(Key(7), clock);
        byte[] token = Mint(authority, 1, 1, 100);
        Assert.Equal(SessionTokenStatus.Valid, authority.TryValidate(token, out _, out _));
        clock.Set(100);
        Assert.Equal(SessionTokenStatus.Expired, authority.TryValidate(token, out _, out _));
    }

    [Fact]
    public void Rotation_Accepts_Previous_Key_Until_Deadline()
    {
        VirtualClock clock = new(0);
        byte[] keyA = Key(0x0A), keyB = Key(0x0B), keyC = Key(0x0C);
        using SessionTokenAuthority authority = new(keyA, clock);
        byte[] underA = Mint(authority, 1, 1, 1_000_000);

        authority.RotateKey(keyB, previousKeyValidUntilMicros: 1000);
        byte[] underB = Mint(authority, 2, 1, 1_000_000);
        Assert.Equal(HMACSHA256.HashData(keyB, underB.AsSpan(0, 37)), underB.AsSpan(37).ToArray());
        Assert.Equal(SessionTokenStatus.Valid, authority.TryInspect(underA, out ulong id, out _));
        Assert.Equal(1UL, id);
        Assert.Equal(SessionTokenStatus.Valid, authority.TryInspect(underB, out _, out _));

        clock.Set(1000);
        Assert.Equal(SessionTokenStatus.BadSignature, authority.TryInspect(underA, out _, out _));
        Assert.Equal(SessionTokenStatus.Valid, authority.TryInspect(underB, out _, out _));

        clock.Set(0);
        authority.RotateKey(keyC, previousKeyValidUntilMicros: 5000);
        Assert.Equal(SessionTokenStatus.BadSignature, authority.TryInspect(underA, out _, out _));
        Assert.Equal(SessionTokenStatus.Valid, authority.TryInspect(underB, out _, out _));
        Assert.Equal(SessionTokenStatus.Valid, authority.TryValidate(Mint(authority, 3, 1, 10), out _, out _));
    }

    [Fact]
    public void Two_Key_Constructor_Accepts_Previous_Key()
    {
        VirtualClock clock = new(0);
        using SessionTokenAuthority old = new(Key(0x10), clock);
        byte[] token = Mint(old, 9, 2, 1_000_000);
        using SessionTokenAuthority authority = new(Key(0x11), Key(0x10), 50, clock);
        Assert.Equal(SessionTokenStatus.Valid, authority.TryInspect(token, out ulong id, out uint epoch));
        Assert.Equal((9UL, 2u), (id, epoch));
        clock.Set(50);
        Assert.Equal(SessionTokenStatus.BadSignature, authority.TryInspect(token, out _, out _));
    }

    [Fact]
    public void Replay_Cache_Capacity_Fails_Closed_Until_Entries_Expire()
    {
        VirtualClock clock = new(0);
        using SessionTokenAuthority authority = new(Key(0x20), clock, replayCacheCapacity: 4);
        Assert.Equal(4, authority.ReplayCacheCapacity);
        byte[][] tokens = [Mint(authority, 1, 1, 100), Mint(authority, 2, 1, 200), Mint(authority, 3, 1, 300), Mint(authority, 4, 1, 400)];
        foreach (byte[] token in tokens)
        {
            Assert.Equal(SessionTokenStatus.Valid, authority.TryValidate(token, out _, out _));
        }

        byte[] fifth = Mint(authority, 5, 1, 500);
        Assert.Equal(SessionTokenStatus.ReplayCacheFull, authority.TryValidate(fifth, out ulong id, out _));
        Assert.Equal(0UL, id);
        Assert.Equal(SessionTokenStatus.ReplayCacheFull, authority.TryValidate(fifth, out _, out _));
        Assert.Equal(SessionTokenStatus.Replayed, authority.TryValidate(tokens[3], out _, out _));

        clock.Set(150); // the first entry expires
        Assert.Equal(SessionTokenStatus.Valid, authority.TryValidate(fifth, out id, out _));
        Assert.Equal(5UL, id);
        Assert.Equal(SessionTokenStatus.Replayed, authority.TryValidate(tokens[1], out _, out _));
        Assert.Equal(SessionTokenStatus.Replayed, authority.TryValidate(fifth, out _, out _));
        Assert.Equal(4, authority.ReplayCacheCount);
    }

    [Fact]
    public void Concurrent_Validation_Of_One_Token_Succeeds_Once()
    {
        VirtualClock clock = new(0);
        using SessionTokenAuthority authority = new(Key(0x30), clock);
        for (int round = 0; round < 20; round++)
        {
            byte[] token = Mint(authority, (ulong)round, 1, 1_000_000);
            int valid = 0;
            Parallel.For(0, 16, attempt =>
            {
                if (authority.TryValidate(token, out _, out _) == SessionTokenStatus.Valid)
                {
                    Interlocked.Increment(ref valid);
                }
            });
            Assert.Equal(1, valid);
        }
    }

    [Fact]
    public void Argument_Validation()
    {
        VirtualClock clock = new();
        Assert.Throws<ArgumentException>(() => new SessionTokenAuthority(new byte[31], clock));
        Assert.Throws<ArgumentException>(() => new SessionTokenAuthority(new byte[33], clock));
        Assert.Throws<ArgumentException>(() => new SessionTokenAuthority(Key(1), new byte[16], 0, clock));
        Assert.Throws<ArgumentNullException>(() => new SessionTokenAuthority(Key(1), null!));
        Assert.Throws<ArgumentOutOfRangeException>(() => new SessionTokenAuthority(Key(1), clock, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new SessionTokenAuthority(Key(1), clock, ReplayCache.MaxCapacity + 1));

        using SessionTokenAuthority authority = new(Key(1), clock);
        Assert.Throws<ArgumentException>(() => authority.Mint(1, 1, 1, new byte[68]));
        Assert.Throws<ArgumentException>(() => authority.RotateKey(new byte[8], 0));
    }

    [Fact]
    public void Dispose_Wipes_Keys_And_Blocks_Use()
    {
        VirtualClock clock = new();
        SessionTokenAuthority authority = new(Key(1), Key(2), 100, clock);
        authority.RotateKey(Key(3), 100);
        byte[] token = Mint(authority, 1, 1, 100);
        authority.Dispose();
        authority.Dispose();
        Assert.Throws<ObjectDisposedException>(() => authority.Mint(1, 1, 1, new byte[69]));
        Assert.Throws<ObjectDisposedException>(() => authority.TryValidate(token, out _, out _));
        Assert.Throws<ObjectDisposedException>(() => authority.TryInspect(token, out _, out _));
        Assert.Throws<ObjectDisposedException>(() => authority.RotateKey(Key(4), 0));

        SessionTokenAuthority single = new(Key(1), clock);
        single.Dispose();
        Assert.Throws<ObjectDisposedException>(() => single.TryInspect(token, out _, out _));
    }

    [Fact]
    public void Retired_Keys_Are_Wiped_Only_When_The_Last_Lease_Is_Released()
    {
        VirtualClock clock = new(0);
        SessionTokenAuthority authority = new(Key(0x51), Key(0x52), long.MaxValue, clock);
        SessionTokenAuthority.KeySet first = authority.AcquireKeys(); // an in-flight validation
        authority.RotateKey(Key(0x53), long.MaxValue);
        Assert.False(first.IsWiped);
        Assert.Equal(Key(0x51), first.Current);
        Assert.Equal(Key(0x52), first.Previous);

        SessionTokenAuthority.KeySet second = authority.AcquireKeys();
        Assert.NotSame(first, second);
        Assert.Equal(Key(0x53), second.Current);
        Assert.Equal(Key(0x51), second.Previous);
        Assert.NotSame(first.Current, second.Previous); // every set owns private copies

        first.Release();
        Assert.True(first.IsWiped);
        Assert.All(first.Current, b => Assert.Equal(0, b));
        Assert.All(first.Previous!, b => Assert.Equal(0, b));
        Assert.Equal(Key(0x51), second.Previous); // the newer set's copy of the outgoing key is untouched

        authority.Dispose();
        Assert.False(second.IsWiped); // still leased
        Assert.Equal(Key(0x53), second.Current);
        second.Release();
        Assert.True(second.IsWiped);
        Assert.All(second.Current, b => Assert.Equal(0, b));
        Assert.All(second.Previous!, b => Assert.Equal(0, b));
    }

    [Fact]
    public void Unleased_Retired_Keys_Are_Wiped_At_Once_And_Wiping_Is_Idempotent()
    {
        VirtualClock clock = new(0);
        using SessionTokenAuthority authority = new(Key(0x61), clock);
        SessionTokenAuthority.KeySet keys = authority.AcquireKeys();
        keys.Release();
        Assert.False(keys.IsWiped);

        authority.RotateKey(Key(0x62), long.MaxValue);
        Assert.True(keys.IsWiped);
        Assert.All(keys.Current, b => Assert.Equal(0, b));
        Assert.Null(keys.Previous);

        // A lease attempt that raced the retirement counts a reference on the retired set and then drops it.
        keys.AddReference();
        keys.Release();
        Assert.True(keys.IsWiped);

        // The outgoing key still verifies through the new set's own copy.
        using SessionTokenAuthority old = new(Key(0x61), clock);
        byte[] token = Mint(old, 5, 1, 1_000);
        Assert.Equal(SessionTokenStatus.Valid, authority.TryInspect(token, out ulong id, out _));
        Assert.Equal(5UL, id);
    }

    [Fact]
    public void Concurrent_Rotation_And_Dispose_Never_Accept_A_Zero_Key_Forgery()
    {
        byte[] forged = new byte[SessionTokenAuthority.TokenLength];
        forged[0] = SessionTokenAuthority.FormatVersion;
        BinaryPrimitives.WriteUInt64LittleEndian(forged.AsSpan(1), 0xBAD_5E55);
        BinaryPrimitives.WriteUInt32LittleEndian(forged.AsSpan(9), 1);
        BinaryPrimitives.WriteInt64LittleEndian(forged.AsSpan(13), long.MaxValue);
        HMACSHA256.HashData(new byte[SessionTokenAuthority.KeyLength], forged.AsSpan(0, 37), forged.AsSpan(37, 32));

        int totalFreshValid = 0;
        for (int round = 0; round < 10; round++)
        {
            VirtualClock clock = new(0);
            SessionTokenAuthority authority = new(Key(0x70), Key(0x71), long.MaxValue, clock);
            int started = 0;
            int forgedAccepted = 0;
            int freshValid = 0;
            int freshOther = 0;
            Thread[] validators = new Thread[4];
            for (int v = 0; v < validators.Length; v++)
            {
                validators[v] = new Thread(() =>
                {
                    byte[] scratch = new byte[SessionTokenAuthority.TokenLength];
                    Interlocked.Increment(ref started);
                    while (true)
                    {
                        try
                        {
                            if (authority.TryInspect(forged, out _, out _) == SessionTokenStatus.Valid)
                            {
                                Interlocked.Increment(ref forgedAccepted);
                            }

                            // A fresh token verifies unless two rotations dropped its key in between (BadSignature).
                            authority.Mint(1, 1, long.MaxValue, scratch);
                            SessionTokenStatus fresh = authority.TryInspect(scratch, out _, out _);
                            if (fresh == SessionTokenStatus.Valid)
                            {
                                Interlocked.Increment(ref freshValid);
                            }
                            else if (fresh != SessionTokenStatus.BadSignature)
                            {
                                Interlocked.Increment(ref freshOther);
                            }
                        }
                        catch (ObjectDisposedException)
                        {
                            return;
                        }
                    }
                });
                validators[v].Start();
            }

            while (Volatile.Read(ref started) < validators.Length)
            {
                Thread.Yield();
            }

            for (int i = 0; i < 300; i++)
            {
                authority.RotateKey(Key((byte)(0x72 + (i % 100))), long.MaxValue);
                Thread.SpinWait(2_000);
            }

            authority.Dispose();
            foreach (Thread validator in validators)
            {
                validator.Join();
            }

            Assert.True(forgedAccepted == 0, $"Round {round}: zero-key forgery accepted {forgedAccepted} times.");
            Assert.True(freshOther == 0, $"Round {round}: {freshOther} fresh tokens failed with an unexpected status.");
            totalFreshValid += freshValid;
        }

        Assert.True(totalFreshValid > 0, "No freshly minted token verified during rotation.");
    }

    [Fact]
    public void Token_Comparisons_Use_FixedTimeEquals()
    {
        // Code inspection: the MAC check and the auth-token helper must call CryptographicOperations.FixedTimeEquals,
        // and the authority must not fall back to an early-exit comparison.
        Assert.True(Calls(typeof(SessionTokenAuthority), typeof(CryptographicOperations), nameof(CryptographicOperations.FixedTimeEquals)));
        Assert.True(Calls(typeof(AuthToken), typeof(CryptographicOperations), nameof(CryptographicOperations.FixedTimeEquals)));
        Assert.False(Calls(typeof(SessionTokenAuthority), typeof(MemoryExtensions), nameof(MemoryExtensions.SequenceEqual)));
    }

    [Fact]
    public void AuthToken_FixedTimeEquals()
    {
        Assert.Equal(4096, AuthToken.MaxLength);
        Assert.True(AuthToken.FixedTimeEquals("secret"u8, "secret"u8));
        Assert.False(AuthToken.FixedTimeEquals("secret"u8, "secreT"u8));
        Assert.False(AuthToken.FixedTimeEquals("secret"u8, "secret!"u8));
        Assert.True(AuthToken.FixedTimeEquals([], []));
    }

    private static bool Calls(Type type, Type targetType, string targetName)
    {
        const BindingFlags all = BindingFlags.Instance | BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.DeclaredOnly;
        foreach (MethodBase method in type.GetMethods(all).Cast<MethodBase>().Concat(type.GetConstructors(all)))
        {
            byte[]? il = method.GetMethodBody()?.GetILAsByteArray();
            if (il is null)
            {
                continue;
            }

            for (int i = 0; i + 4 < il.Length; i++)
            {
                if (il[i] is not (0x28 or 0x6F)) // call, callvirt
                {
                    continue;
                }

                int token = BitConverter.ToInt32(il, i + 1);
                try
                {
                    MethodBase? target = method.Module.ResolveMethod(token);
                    if (target?.DeclaringType == targetType && target.Name == targetName)
                    {
                        return true;
                    }
                }
                catch (ArgumentException)
                {
                    // Not a method token (the byte was part of another instruction's operand).
                }
            }
        }

        return false;
    }
}
