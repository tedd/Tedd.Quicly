using System.Buffers.Binary;

namespace Tedd.Quicly.Core.Tests.Primitives;

/// <summary>Deterministic input generators shared by the primitive tests.</summary>
internal static class PrimitiveTestData
{
    public static byte[] Random(int length, int seed)
    {
        byte[] data = new byte[length];
        new Random(seed).NextBytes(data);
        return data;
    }

    /// <summary>A short random pattern repeated with occasional single-byte mutations.</summary>
    public static byte[] Repetitive(int length, int seed, int period = 37)
    {
        Random random = new(seed);
        byte[] pattern = new byte[period];
        random.NextBytes(pattern);
        byte[] data = new byte[length];
        for (int i = 0; i < length; i++)
        {
            data[i] = pattern[i % period];
            if (random.Next(0, 500) == 0)
            {
                data[i] ^= 0x5A;
            }
        }

        return data;
    }

    /// <summary>
    /// Game-like snapshot data: 32-byte entity records with incrementing ids, slowly drifting positions,
    /// small-alphabet type/flag bytes and zero padding. Structured and partially repetitive.
    /// </summary>
    public static byte[] GameLike(int length, int seed)
    {
        Random random = new(seed);
        byte[] data = new byte[length];
        Span<byte> record = stackalloc byte[32];
        float x = 100f, y = 20f, z = -50f;
        uint id = 1000;
        int written = 0;
        while (written < length)
        {
            record.Clear();
            BinaryPrimitives.WriteUInt32LittleEndian(record, id++);
            x += (float)(random.NextDouble() - 0.5);
            y += (float)(random.NextDouble() - 0.5) * 0.1f;
            z += (float)(random.NextDouble() - 0.5);
            BinaryPrimitives.WriteSingleLittleEndian(record.Slice(4), x);
            BinaryPrimitives.WriteSingleLittleEndian(record.Slice(8), y);
            BinaryPrimitives.WriteSingleLittleEndian(record.Slice(12), z);
            BinaryPrimitives.WriteInt16LittleEndian(record.Slice(16), (short)random.Next(-3, 4));
            BinaryPrimitives.WriteInt16LittleEndian(record.Slice(18), (short)random.Next(-3, 4));
            BinaryPrimitives.WriteInt16LittleEndian(record.Slice(20), (short)random.Next(-3, 4));
            record[22] = (byte)random.Next(0, 6);
            record[23] = (byte)(random.Next(0, 8) == 0 ? 1 : 0);
            BinaryPrimitives.WriteUInt16LittleEndian(record.Slice(24), (ushort)(random.Next(0, 3) == 0 ? random.Next(0, 100) : 100));
            int n = Math.Min(record.Length, length - written);
            record.Slice(0, n).CopyTo(data.AsSpan(written));
            written += n;
        }

        return data;
    }
}
