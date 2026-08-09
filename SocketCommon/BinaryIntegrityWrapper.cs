using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

namespace SocketCommon;

/// <summary>
/// Binary Integrity Wrapper — 端到端資料完整性協議 (Gateway 透明通過)
/// 
/// Header format (16 bytes):
/// ┌─────────┬──────────┬──────────┬───────────┬──────────┐
/// │ Magic   │ SeqNo    │ Flags    │ CRC32     │ Length   │
/// │ 4 bytes │ 4 bytes  │ 2 bytes  │ 4 bytes   │ 2 bytes │
/// └─────────┴──────────┴──────────┴───────────┴──────────┘
/// 
/// Flags: ECHO=0x01, ROUTING=0x02, HASH=0x04
/// </summary>
public static class BinaryIntegrityWrapper
{
    public const uint Magic = 0x49_4E_54_47;
    public const int HeaderSize = 16;

    public const short FlagEcho = 0x01;
    public const short FlagRouting = 0x02;
    public const short FlagHash = 0x04;

    // ── Wrap methods ─────────────────────────────────────

    /// <summary>Wrap payload with integrity header + CRC32.</summary>
    public static byte[] Wrap(byte[] payload, int seqNo, short flags = 0)
    {
        if (payload == null || payload.Length == 0)
            throw new ArgumentException("Payload must not be empty.");

        var crc = ComputeCrc32(payload);
        var header = new byte[HeaderSize];
        BinaryPrimitives.WriteUInt32BigEndian(header.AsSpan(0), Magic);
        BinaryPrimitives.WriteInt32BigEndian(header.AsSpan(4), seqNo);
        BinaryPrimitives.WriteInt16BigEndian(header.AsSpan(8), flags);
        BinaryPrimitives.WriteUInt32BigEndian(header.AsSpan(10), crc);
        BinaryPrimitives.WriteUInt16BigEndian(header.AsSpan(14), (ushort)payload.Length);

        var result = new byte[HeaderSize + payload.Length];
        Array.Copy(header, 0, result, 0, HeaderSize);
        Array.Copy(payload, 0, result, HeaderSize, payload.Length);
        return result;
    }

    /// <summary>Wrap with nodeId trailer for routing verification.</summary>
    public static byte[] WrapWithRoutingCheck(byte[] payload, int seqNo, string expectedNodeId)
    {
        var nodeBytes = Encoding.UTF8.GetBytes(expectedNodeId);
        var combined = new byte[payload.Length + nodeBytes.Length];
        Array.Copy(payload, 0, combined, 0, payload.Length);
        Array.Copy(nodeBytes, 0, combined, payload.Length, nodeBytes.Length);
        return Wrap(combined, seqNo, FlagEcho | FlagRouting);
    }

    /// <summary>Wrap with SHA256 hash trailer.</summary>
    public static byte[] WrapWithHash(byte[] payload, int seqNo)
    {
        var hash = SHA256.HashData(payload);
        var combined = new byte[payload.Length + hash.Length];
        Array.Copy(payload, 0, combined, 0, payload.Length);
        Array.Copy(hash, 0, combined, payload.Length, hash.Length);
        return Wrap(combined, seqNo, FlagEcho | FlagHash);
    }

    // ── Unwrap method ────────────────────────────────────

    /// <summary>Unwrap a received buffer.</summary>
    public static (bool success, bool validCrc, int seqNo, short flags, byte[] data) Unwrap(byte[] buffer)
    {
        if (buffer == null || buffer.Length < HeaderSize)
            return (false, false, 0, 0, Array.Empty<byte>());

        var magic = BinaryPrimitives.ReadUInt32BigEndian(buffer.AsSpan(0));
        if (magic != Magic)
            return (false, false, 0, 0, Array.Empty<byte>());

        var seqNo = BinaryPrimitives.ReadInt32BigEndian(buffer.AsSpan(4));
        var flags = BinaryPrimitives.ReadInt16BigEndian(buffer.AsSpan(8));
        var expectedCrc = BinaryPrimitives.ReadUInt32BigEndian(buffer.AsSpan(10));
        var length = BinaryPrimitives.ReadUInt16BigEndian(buffer.AsSpan(14));

        if (buffer.Length < HeaderSize + length)
            return (false, false, seqNo, flags, Array.Empty<byte>());

        var data = new byte[length];
        Array.Copy(buffer, HeaderSize, data, 0, length);

        return (true, ComputeCrc32(data) == expectedCrc, seqNo, flags, data);
    }

    // ── Helpers ──────────────────────────────────────────

    /// <summary>Extract nodeId from routing-check response payload.</summary>
    public static string? ExtractNodeId(byte[] data)
    {
        if (data.Length < 1) return null;
        var nodeStr = Encoding.UTF8.GetString(data);
        var lastColon = nodeStr.LastIndexOf(':');
        if (lastColon >= 0 && lastColon < nodeStr.Length - 1)
            return nodeStr.Substring(lastColon + 1).Trim();
        return nodeStr.Trim();
    }

    /// <summary>Verify SHA256 hash trailer (last 32 bytes).</summary>
    public static bool VerifyHash(byte[] data)
    {
        if (data.Length < 33) return false;
        var dataLen = data.Length - 32;
        return SHA256.HashData(data.AsSpan(0, dataLen)).SequenceEqual(data.AsSpan(dataLen, 32));
    }

    /// <summary>Try to read one complete framed message. Returns null if incomplete.</summary>
    public static (int consumedBytes, bool success, bool validCrc, int seqNo, short flags, byte[] data)? TryReadFrame(byte[] buffer, int offset, int count)
    {
        var available = count - offset;
        if (available < HeaderSize) return null;

        var magic = BinaryPrimitives.ReadUInt32BigEndian(buffer.AsSpan(offset));
        if (magic != Magic) return null;

        var length = BinaryPrimitives.ReadUInt16BigEndian(buffer.AsSpan(offset + 14));
        var totalNeeded = HeaderSize + length;
        if (available < totalNeeded) return null;

        var packet = new byte[totalNeeded];
        Array.Copy(buffer, offset, packet, 0, totalNeeded);

        var (success, validCrc, seqNo, flags, data) = Unwrap(packet);
        return (totalNeeded, success, validCrc, seqNo, flags, data);
    }

    /// <summary>Parse multiple framed messages from a stream buffer.</summary>
    public static List<(int seqNo, short flags, bool validCrc, byte[] data)> ParseStream(byte[] buffer)
    {
        var results = new List<(int, short, bool, byte[])>();
        var offset = 0;

        while (offset < buffer.Length)
        {
            var frame = TryReadFrame(buffer, offset, buffer.Length);
            if (frame == null) break;

            offset += frame.Value.consumedBytes;
            if (frame.Value.success && frame.Value.validCrc)
                results.Add((frame.Value.seqNo, frame.Value.flags, true, frame.Value.data));
        }

        return results;
    }

    /// <summary>Generate random payload for testing.</summary>
    public static byte[] GenerateRandomPayload(int minSize, int maxSize)
    {
        var size = Random.Shared.Next(minSize, maxSize + 1);
        var data = new byte[size];
        Random.Shared.NextBytes(data);
        return data;
    }

    // ── CRC32 Implementation ─────────────────────────────

    private static readonly uint[] CrcTable = BuildCrcTable();

    private static uint[] BuildCrcTable()
    {
        var table = new uint[256];
        for (int i = 0; i < 256; i++)
        {
            var crc = (uint)i;
            for (int j = 0; j < 8; j++)
                crc = (crc >> 1) ^ (uint)(0xEDB8_8320 * (crc & 1));
            table[i] = crc;
        }
        return table;
    }

    private static uint ComputeCrc32(byte[] data)
    {
        var crc = 0xFFFFFFFFu;
        foreach (var b in data)
            crc = (crc >> 8) ^ CrcTable[(crc ^ b) & 0xFF];
        return ~crc;
    }
}