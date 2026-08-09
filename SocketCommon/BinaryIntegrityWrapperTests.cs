using System;
using System.Linq;
using Xunit;

namespace SocketCommon.Tests;

/// <summary>
/// Unit tests for BinaryIntegrityWrapper — verify wrap/unwrap, CRC, hash, and frame parsing.
/// </summary>
public class BinaryIntegrityWrapperTests
{
    // ── Basic Wrap / Unwrap ────────────────────────────────

    [Fact]
    public void Wrap_Unwrap_ShouldPreserveData()
    {
        var payload = new byte[] { 1, 2, 3, 4, 5 };
        var wrapped = BinaryIntegrityWrapper.Wrap(payload, seqNo: 42);

        Assert.True(wrapped.Length > payload.Length); // Header added

        var (success, validCrc, seqNo, flags, data) = BinaryIntegrityWrapper.Unwrap(wrapped);

        Assert.True(success);
        Assert.True(validCrc);
        Assert.Equal(42, seqNo);
        Assert.Equal((short)0, flags);
        Assert.Equal(payload, data);
    }

    [Fact]
    public void Wrap_ShouldSetCorrectHeaderFields()
    {
        var payload = new byte[] { 0xDE, 0xAD };
        var seqNo = 99;
        var flags = (short)(BinaryIntegrityWrapper.FlagEcho | BinaryIntegrityWrapper.FlagRouting);

        var wrapped = BinaryIntegrityWrapper.Wrap(payload, seqNo, flags);

        var (success, validCrc, readSeq, readFlags, data) = BinaryIntegrityWrapper.Unwrap(wrapped);

        Assert.True(success);
        Assert.True(validCrc);
        Assert.Equal(seqNo, readSeq);
        Assert.Equal(flags, readFlags);
    }

    [Fact]
    public void Wrap_LargePayload_ShouldWork()
    {
        var payload = BinaryIntegrityWrapper.GenerateRandomPayload(1024, 65535);
        var wrapped = BinaryIntegrityWrapper.Wrap(payload, 0);

        Assert.Equal(BinaryIntegrityWrapper.HeaderSize + payload.Length, wrapped.Length);

        var (success, validCrc, _, _, data) = BinaryIntegrityWrapper.Unwrap(wrapped);
        Assert.True(success);
        Assert.True(validCrc);
        Assert.Equal(payload, data);
    }

    // ── CRC Validation ─────────────────────────────────────

    [Fact]
    public void Unwrap_CorruptedData_ShouldDetectInvalidCrc()
    {
        var payload = new byte[] { 1, 2, 3, 4 };
        var wrapped = BinaryIntegrityWrapper.Wrap(payload, 0);

        // Corrupt a byte in the payload section (after header)
        wrapped[BinaryIntegrityWrapper.HeaderSize] ^= 0xFF;

        var (success, validCrc, _, _, _) = BinaryIntegrityWrapper.Unwrap(wrapped);

        Assert.True(success); // Header still parseable
        Assert.False(validCrc); // CRC should fail
    }

    [Fact]
    public void Unwrap_CorruptedHeader_ShouldFail()
    {
        var payload = new byte[] { 1, 2, 3 };
        var wrapped = BinaryIntegrityWrapper.Wrap(payload, 0);

        // Corrupt magic number
        wrapped[0] ^= 0xFF;

        var (success, _, _, _, _) = BinaryIntegrityWrapper.Unwrap(wrapped);

        Assert.False(success);
    }

    [Fact]
    public void Unwrap_BadCrcInHeader_ShouldDetect()
    {
        var payload = new byte[] { 10, 20, 30 };
        var wrapped = BinaryIntegrityWrapper.Wrap(payload, 0);

        // Flip a bit in the CRC field (bytes 10-13)
        wrapped[10] ^= 0x01;

        var (_, validCrc, _, _, _) = BinaryIntegrityWrapper.Unwrap(wrapped);

        Assert.False(validCrc);
    }

    // ── Hash Verification ──────────────────────────────────

    [Fact]
    public void WrapWithHash_VerifyHash_ShouldPass()
    {
        var payload = new byte[] { 0xAA, 0xBB, 0xCC };
        var wrapped = BinaryIntegrityWrapper.WrapWithHash(payload, 1);

        var (_, validCrc, _, flags, data) = BinaryIntegrityWrapper.Unwrap(wrapped);

        Assert.True(validCrc);
        Assert.Contains(BinaryIntegrityWrapper.FlagHash, flags);

        // Verify the hash trailer matches
        Assert.True(BinaryIntegrityWrapper.VerifyHash(data));
    }

    [Fact]
    public void WrapWithHash_CorruptedTrailer_ShouldFail()
    {
        var payload = BinaryIntegrityWrapper.GenerateRandomPayload(64, 256);
        var wrapped = BinaryIntegrityWrapper.WrapWithHash(payload, 1);

        var (_, _, _, _, data) = BinaryIntegrityWrapper.Unwrap(wrapped);

        // Corrupt the last byte (part of hash trailer)
        data[^1] ^= 0xFF;

        Assert.False(BinaryIntegrityWrapper.VerifyHash(data));
    }

    [Fact]
    public void WrapWithHash_CorruptedPayload_ShouldFail()
    {
        var payload = BinaryIntegrityWrapper.GenerateRandomPayload(64, 256);
        var wrapped = BinaryIntegrityWrapper.WrapWithHash(payload, 1);

        var (_, _, _, _, data) = BinaryIntegrityWrapper.Unwrap(wrapped);

        // Corrupt a byte in the payload section (not the hash trailer)
        data[0] ^= 0xFF;

        Assert.False(BinaryIntegrityWrapper.VerifyHash(data));
    }

    // ── Routing Check ──────────────────────────────────────

    [Fact]
    public void WrapWithRoutingCheck_ShouldIncludeNodeId()
    {
        var payload = new byte[] { 1, 2, 3 };
        var nodeId = "server-1:5001";
        var wrapped = BinaryIntegrityWrapper.WrapWithRoutingCheck(payload, 7, nodeId);

        var (_, validCrc, seqNo, flags, data) = BinaryIntegrityWrapper.Unwrap(wrapped);

        Assert.True(validCrc);
        Assert.Equal(7, seqNo);
        Assert.Contains(BinaryIntegrityWrapper.FlagRouting, flags);

        // The nodeId should be extractable from the data
        var extracted = BinaryIntegrityWrapper.ExtractNodeId(data);
        Assert.NotNull(extracted);
        Assert.Contains("5001", extracted!);
    }

    // ── Frame Parsing ──────────────────────────────────────

    [Fact]
    public void TryReadFrame_Incomplete_ShouldReturnNull()
    {
        var payload = new byte[] { 1, 2, 3 };
        var wrapped = BinaryIntegrityWrapper.Wrap(payload, 0);

        // Feed only part of the header
        var partial = wrapped[..5];
        var result = BinaryIntegrityWrapper.TryReadFrame(partial, 0, partial.Length);

        Assert.Null(result);
    }

    [Fact]
    public void TryReadFrame_Complete_ShouldParse()
    {
        var payload = new byte[] { 42 };
        var wrapped = BinaryIntegrityWrapper.Wrap(payload, 123);

        var result = BinaryIntegrityWrapper.TryReadFrame(wrapped, 0, wrapped.Length);

        Assert.NotNull(result);
        Assert.Equal(wrapped.Length, result.Value.consumed);
        Assert.True(result.Value.result.success);
        Assert.Equal(123, result.Value.result.seqNo);
    }

    [Fact]
    public void ParseStream_MultipleFrames_ShouldExtractAll()
    {
        // Create three frames and concatenate
        var f1 = BinaryIntegrityWrapper.Wrap(new byte[] { 1 }, seqNo: 1);
        var f2 = BinaryIntegrityWrapper.Wrap(new byte[] { 2, 3 }, seqNo: 2);
        var f3 = BinaryIntegrityWrapper.Wrap(new byte[] { 4, 5, 6 }, seqNo: 3);

        var stream = new byte[f1.Length + f2.Length + f3.Length];
        var offset = 0;
        Array.Copy(f1, 0, stream, offset, f1.Length); offset += f1.Length;
        Array.Copy(f2, 0, stream, offset, f2.Length); offset += f2.Length;
        Array.Copy(f3, 0, stream, offset, f3.Length);

        var results = BinaryIntegrityWrapper.ParseStream(stream);

        Assert.Equal(3, results.Count);
        Assert.Equal(1, results[0].seqNo);
        Assert.Equal(2, results[1].seqNo);
        Assert.Equal(3, results[2].seqNo);
    }

    [Fact]
    public void ParseStream_WithTrailingIncompleteFrame_ShouldParseCompleteOnes()
    {
        var f1 = BinaryIntegrityWrapper.Wrap(new byte[] { 10 }, seqNo: 1);
        var f2 = BinaryIntegrityWrapper.Wrap(new byte[] { 20, 30 }, seqNo: 2);

        // Cut off the last frame's data bytes (incomplete)
        var stream = new byte[f1.Length + f2.Length - 2];
        Array.Copy(f1, 0, stream, 0, f1.Length);
        Array.Copy(f2, 0, stream, f1.Length, f2.Length - 2);

        var results = BinaryIntegrityWrapper.ParseStream(stream);

        // Only first frame should be parsed
        Assert.Single(results);
        Assert.Equal(1, results[0].seqNo);
    }

    [Fact]
    public void GenerateRandomPayload_ShouldBeRandomAndCorrectSize()
    {
        var data = BinaryIntegrityWrapper.GenerateRandomPayload(100, 200);

        Assert.InRange(data.Length, 100, 200);
    }

    [Fact]
    public void Wrap_EmptyPayload_ShouldThrow()
    {
        Assert.Throws<ArgumentException>(() =>
            BinaryIntegrityWrapper.Wrap(Array.Empty<byte>(), 0));
    }

    [Fact]
    public void Unwrap_TooShort_ShouldFail()
    {
        var shortBuf = new byte[] { 1, 2, 3 }; // Less than HeaderSize (16)
        var (success, _, _, _, _) = BinaryIntegrityWrapper.Unwrap(shortBuf);
        Assert.False(success);
    }

    [Fact]
    public void Unwrap_IncompletePayload_ShouldFail()
    {
        var payload = new byte[100];
        var wrapped = BinaryIntegrityWrapper.Wrap(payload, 0);

        // Only send header + partial data
        var truncated = wrapped[..30];
        var (success, _, _, _, data) = BinaryIntegrityWrapper.Unwrap(truncated);

        Assert.True(success); // Header parsed OK
        Assert.Empty(data);   // But no data due to incomplete packet
    }

    // ── Edge Cases ─────────────────────────────────────────

    [Fact]
    public void Wrap_MaxUshortPayload_ShouldWork()
    {
        var payload = new byte[ushort.MaxValue];
        Random.Shared.NextBytes(payload);
        var wrapped = BinaryIntegrityWrapper.Wrap(payload, 0);

        Assert.Equal(BinaryIntegrityWrapper.HeaderSize + ushort.MaxValue, wrapped.Length);

        var (_, validCrc, _, _, data) = BinaryIntegrityWrapper.Unwrap(wrapped);
        Assert.True(validCrc);
        Assert.Equal(ushort.MaxValue, data.Length);
    }

    [Fact]
    public void Wrap_SingleByte_ShouldWork()
    {
        var payload = new byte[] { 0x42 };
        var wrapped = BinaryIntegrityWrapper.Wrap(payload, 0);

        var (_, validCrc, _, _, data) = BinaryIntegrityWrapper.Unwrap(wrapped);
        Assert.True(validCrc);
        Assert.Single(data);
        Assert.Equal(0x42, data[0]);
    }

    [Fact]
    public void VerifyHash_DataTooSmall_ShouldReturnFalse()
    {
        // Less than 33 bytes (1 byte data + 32 byte hash minimum)
        Assert.False(BinaryIntegrityWrapper.VerifyHash(new byte[32]));
        Assert.False(BinaryIntegrityWrapper.VerifyHash(Array.Empty<byte>()));
    }

    [Fact]
    public void ExtractNodeId_NullOrEmpty_ShouldReturnNull()
    {
        Assert.Null(BinaryIntegrityWrapper.ExtractNodeId(null!));
        Assert.Null(BinaryIntegrityWrapper.ExtractNodeId(Array.Empty<byte>()));
    }
}