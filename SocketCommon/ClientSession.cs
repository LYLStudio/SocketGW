using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;

namespace SocketCommon;

/// <summary>
/// 代表單一客戶端連線的會話資訊
/// </summary>
public sealed class ClientSession : IDisposable
{
    private readonly CancellationTokenSource _cancelSource = new();
    private bool _disposed;

    // Thread-safe counters using private fields
    private long _bytesReceived;
    private long _bytesSent;
    private int _messageCount;

    public string SessionId { get; }
    public Socket Socket { get; }
    public EndPoint? RemoteEndPoint { get; }
    public DateTime ConnectedAt { get; }
    public long BytesReceived => Volatile.Read(ref _bytesReceived);
    public long BytesSent => Volatile.Read(ref _bytesSent);
    public int MessageCount => Volatile.Read(ref _messageCount);

    // 使用 Channel 作為高效能的 lock-free 訊息佇列
    public Channel<Message> MessageQueue { get; }

    public ClientSession(string sessionId, Socket socket)
    {
        SessionId = sessionId;
        Socket = socket;
        RemoteEndPoint = socket.RemoteEndPoint;
        ConnectedAt = DateTime.UtcNow;
        MessageQueue = Channel.CreateUnbounded<Message>(
            new UnboundedChannelOptions
            {
                AllowSynchronousContinuations = true,
                SingleReader = true,
                SingleWriter = false,
            });
    }

    public void RecordReceive(int byteCount)
    {
        Interlocked.Add(ref _bytesReceived, byteCount);
        Interlocked.Increment(ref _messageCount);
    }

    public void RecordSend(int byteCount)
    {
        Interlocked.Add(ref _bytesSent, byteCount);
    }

    public CancellationToken Token => _cancelSource.Token;

    public void Disconnect()
    {
        _cancelSource.Cancel();
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        try { Socket.Shutdown(SocketShutdown.Both); } catch { }
        try { Socket.Close(); } catch { }

        _cancelSource.Cancel();
        _cancelSource.Dispose();
    }
}

/// <summary>
/// 訊息類型定義
/// </summary>
public readonly struct Message
{
    public enum MessageType
    {
        Data,
        Close,
        Ping,
        Pong,
    }

    public MessageType Type { get; init; }
    public ReadOnlyMemory<byte>? Payload { get; init; }
    public string? TextPayload { get; init; }

    public static Message CreateData(ReadOnlyMemory<byte> payload) => new()
    {
        Type = MessageType.Data,
        Payload = payload,
    };

    public static Message CreateText(string text) => new()
    {
        Type = MessageType.Data,
        TextPayload = text,
    };

    public static Message CreateClose() => new() { Type = MessageType.Close };
    public static Message CreatePing() => new() { Type = MessageType.Ping };
    public static Message CreatePong() => new() { Type = MessageType.Pong };
}

/// <summary>
/// 伺服器統計資料
/// </summary>
public sealed class ServerStatistics
{
    public int CurrentConnections { get; init; }
    public long TotalConnections { get; init; }
    public long TotalDisconnections { get; init; }
    public long TotalBytesReceived { get; init; }
    public long TotalBytesSent { get; init; }
    public bool IsRunning { get; init; }
    public int SessionCount { get; init; }

    public override string ToString()
    {
        return $"Connections: {CurrentConnections}, Total: {TotalConnections}, " +
               $"RX: {TotalBytesReceived}, TX: {TotalBytesSent}";
    }
}