using System.Buffers;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using SocketCommon;

namespace SocketServerLib;

/// <summary>
/// 高併發 Socket Server，使用 I/O Completion Port (IOCP) 模型
/// Linux: epoll / Windows: IOCP
/// 支援數十萬並行連線
/// </summary>
public sealed class HighPerformanceSocketServer : IDisposable
{
    private Socket _serverSocket;
    private readonly ConcurrentDictionary<string, ClientSession> _sessions = new();
    private readonly CancellationTokenSource _shutdownCts = new();

    // Server 配置
    public int Port { get; }
    public int Backlog { get; }
    public int ReceiveBufferSize { get; }
    public int SendBufferSize { get; }
    public bool DualMode { get; }
    public int MaxConnections { get; }

    // 統計資訊 (thread-safe via Interlocked)
    private long _totalConnections;
    private long _totalDisconnections;
    private long _totalBytesReceived;
    private long _totalBytesSent;
    private int _currentConnectionCount;
    private bool _isRunning;

    // Session ID counter (thread-safe atomic)
    private static long _sessionCounter;

    // Events for external notification
    public event Action<string>? ClientConnected;
    public event Action<string>? ClientDisconnected;

    public HighPerformanceSocketServer(
        int port = 5000,
        int backlog = 10000,
        int receiveBufferSize = 64 * 1024,
        int sendBufferSize = 64 * 1024,
        bool dualMode = false,
        int maxConnections = int.MaxValue)
    {
        Port = port;
        Backlog = backlog;
        ReceiveBufferSize = receiveBufferSize;
        SendBufferSize = sendBufferSize;
        DualMode = dualMode;
        MaxConnections = maxConnections;

        var addressFamily = dualMode ? AddressFamily.InterNetworkV6 : AddressFamily.InterNetwork;
        _serverSocket = CreateSocket(addressFamily, receiveBufferSize, sendBufferSize, dualMode);
    }

    private static Socket CreateSocket(AddressFamily family, int rxBuf, int txBuf, bool dualMode)
    {
        var socket = new Socket(family, SocketType.Stream, ProtocolType.Tcp)
        {
            Blocking = false,
            ReceiveBufferSize = rxBuf,
            SendBufferSize = txBuf,
            NoDelay = true,
        };

        if (family == AddressFamily.InterNetworkV6)
        {
            socket.DualMode = dualMode;
        }

        try
        {
            socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
        }
        catch { /* 某些平台可能不支援 */ }

        return socket;
    }

    public async Task StartAsync()
    {
        _isRunning = true;

        var endPoint = new IPEndPoint(IPAddress.Any, Port);
        _serverSocket.Bind(endPoint);
        _serverSocket.Listen(Backlog);

        await AcceptLoopAsync();
    }

    private async Task AcceptLoopAsync()
    {
        while (!_shutdownCts.Token.IsCancellationRequested && _isRunning)
        {
            try
            {
                var clientSocket = await _serverSocket.AcceptAsync(_shutdownCts.Token);

                if (_currentConnectionCount >= MaxConnections)
                {
                    await RejectConnectionAsync(clientSocket);
                    continue;
                }

                Interlocked.Increment(ref _currentConnectionCount);
                Interlocked.Increment(ref _totalConnections);

                var sessionId = Interlocked.Increment(ref _sessionCounter).ToString();
                var session = new ClientSession(sessionId, clientSocket);
                _sessions[sessionId] = session;

                ClientConnected?.Invoke(sessionId);

                _ = Task.Run(() => HandleClientAsync(session), _shutdownCts.Token);
            }
            catch (OperationCanceledException) { break; }
            catch (ObjectDisposedException) { break; }
            catch (SocketException) when (_shutdownCts.Token.IsCancellationRequested) { break; }
            catch (Exception ex)
            {
                Console.WriteLine($"[AcceptLoop] Error: {ex.Message}");
            }
        }
    }

    private async Task HandleClientAsync(ClientSession session)
    {
        var socket = session.Socket;

        try
        {
            socket.ReceiveBufferSize = ReceiveBufferSize;
            socket.SendBufferSize = SendBufferSize;
            socket.NoDelay = true;

            var poolBuffer = ArrayPool<byte>.Shared.Rent(8192);
            var readBuffer = new Memory<byte>(poolBuffer, 0, 8192);

            try
            {
                while (!_shutdownCts.Token.IsCancellationRequested && !session.Token.IsCancellationRequested)
                {
                    int bytesRead = await socket.ReceiveAsync(readBuffer, SocketFlags.None, session.Token);

                    if (bytesRead == 0) break;

                    Interlocked.Add(ref _totalBytesReceived, bytesRead);
                    session.RecordReceive(bytesRead);

                    await ProcessIncomingDataAsync(session, readBuffer[..bytesRead]);
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(poolBuffer);
            }
        }
        catch (OperationCanceledException) { /* Expected */ }
        catch (SocketException) when (session.Token.IsCancellationRequested) { /* Expected */ }
        catch (Exception ex)
        {
            Console.WriteLine($"[Client {session.SessionId}] Error: {ex.Message}");
        }
        finally
        {
            OnClientDisconnected(session);
        }
    }

    private async Task ProcessIncomingDataAsync(ClientSession session, ReadOnlyMemory<byte> data)
    {
        // Echo back
        var echoBuffer = new byte[data.Length];
        data.CopyTo(echoBuffer);
        await SendRawDataAsync(session, echoBuffer);

        if (data.Length < 4) return;

        var command = TryParseCommand(data.Span[..Math.Min(data.Length, 128)]);
        if (command == null) return;

        ProcessCommand(session, command, data);
    }

    private void ProcessCommand(ClientSession session, string command, ReadOnlyMemory<byte> data)
    {
        var lowerCmd = command.ToLowerInvariant();

        switch (lowerCmd)
        {
            case "ping":
                _ = SendTextAsync(session, "PONG");
                break;
            case "stats":
                _ = SendTextAsync(session, GetStatsText());
                break;
            case "clients":
                _ = SendTextAsync(session, $"Active clients: {_currentConnectionCount}");
                break;
            case "disconnect":
                session.Disconnect();
                break;
            default:
                if (lowerCmd.StartsWith("broadcast"))
                {
                    var message = data.Length > 10
                        ? System.Text.Encoding.UTF8.GetString(data.Span[9..])
                        : "Hello all!";
                    _ = BroadcastAsync(session, message);
                }
                else if (lowerCmd.StartsWith("send:"))
                {
                    var partStr = System.Text.Encoding.UTF8.GetString(
                        data.Span[..Math.Min(data.Length, 256)]);
                    var sendParts = partStr.Split([':'], 3);
                    if (sendParts.Length >= 3 && _sessions.TryGetValue(sendParts[1], out var target))
                    {
                        _ = SendTextAsync(target, sendParts[2]);
                    }
                }
                break;
        }
    }

    private string? TryParseCommand(ReadOnlySpan<byte> span)
    {
        var length = Math.Min(span.Length, 128);
        var chars = new char[length];
        for (int i = 0; i < length; i++)
            chars[i] = (char)span[i];

        var str = new string(chars).Replace('\0', ' ').Trim();
        return string.IsNullOrEmpty(str) ? null : str;
    }

    private async Task SendRawDataAsync(ClientSession session, byte[] data)
    {
        if (data == null || data.Length == 0) return;

        try
        {
            await session.Socket.SendAsync(new ArraySegment<byte>(data), SocketFlags.None, session.Token);
            session.RecordSend(data.Length);
            Interlocked.Add(ref _totalBytesSent, data.Length);
        }
        catch (OperationCanceledException) { /* Client disconnected */ }
        catch (SocketException) { /* Socket closed */ }
    }

    private async Task SendTextAsync(ClientSession session, string text)
    {
        if (string.IsNullOrEmpty(text)) return;
        var bytes = System.Text.Encoding.UTF8.GetBytes(text + "\n");
        await SendRawDataAsync(session, bytes);
    }

    private async Task BroadcastAsync(ClientSession sender, string message)
    {
        var broadcastText = $"[Broadcast from {sender.SessionId}]: {message}\n";
        var bytes = System.Text.Encoding.UTF8.GetBytes(broadcastText);

        var sendTasks = new List<ValueTask<int>>();
        foreach (var session in _sessions.Values)
        {
            if (session.SessionId == sender.SessionId) continue;
            if (session.Token.IsCancellationRequested) continue;

            try
            {
                sendTasks.Add(session.Socket.SendAsync(bytes, SocketFlags.None, session.Token));
            }
            catch { /* Skip failed sends */ }
        }

        foreach (var vt in sendTasks)
        {
            try { await vt; } catch { /* Ignore */ }
        }
    }

    private void OnClientDisconnected(ClientSession session)
    {
        _sessions.TryRemove(session.SessionId, out _);
        Interlocked.Decrement(ref _currentConnectionCount);
        Interlocked.Increment(ref _totalDisconnections);

        ClientDisconnected?.Invoke(session.SessionId);

        session.Dispose();
    }

    private async Task RejectConnectionAsync(Socket clientSocket)
    {
        try
        {
            var rejectMsg = System.Text.Encoding.UTF8.GetBytes("Server at capacity. Please retry later.\n");
            await clientSocket.SendAsync(rejectMsg, SocketFlags.None);
        }
        catch { /* Ignore */ }

        try { clientSocket.Shutdown(SocketShutdown.Both); } catch { /* Ignore */ }
        clientSocket.Close();
    }

    public ServerStatistics GetStatistics()
    {
        return new ServerStatistics
        {
            CurrentConnections = Volatile.Read(ref _currentConnectionCount),
            TotalConnections = Volatile.Read(ref _totalConnections),
            TotalDisconnections = Volatile.Read(ref _totalDisconnections),
            TotalBytesReceived = Volatile.Read(ref _totalBytesReceived),
            TotalBytesSent = Volatile.Read(ref _totalBytesSent),
            IsRunning = _isRunning,
            SessionCount = _sessions.Count
        };
    }

    public IReadOnlyDictionary<string, ClientSession> GetSessions() => _sessions;

    private string GetStatsText()
    {
        var stats = GetStatistics();
        return $"Server Stats:\n" +
               $"  Port: {Port}\n" +
               $"  Active Connections: {stats.CurrentConnections}\n" +
               $"  Total Connected: {stats.TotalConnections}\n" +
               $"  Total Disconnected: {stats.TotalDisconnections}\n" +
               $"  Bytes Received: {FormatBytes(stats.TotalBytesReceived)}\n" +
               $"  Bytes Sent: {FormatBytes(stats.TotalBytesSent)}\n" +
               $"  Status: {(stats.IsRunning ? "Running" : "Stopped")}";
    }

    private static string FormatBytes(long bytes) => bytes switch
    {
        < 1024 => $"{bytes} B",
        < 1024 * 1024 => $"{bytes / 1024.0:F2} KB",
        < 1024 * 1024 * 1024 => $"{bytes / (1024.0 * 1024):F2} MB",
        _ => $"{bytes / (1024.0 * 1024 * 1024):F2} GB"
    };

    public async Task StopAsync()
    {
        if (!_isRunning) return;
        _isRunning = false;
        _shutdownCts.Cancel();

        var disconnectTasks = new List<Task>();
        foreach (var session in _sessions.Values)
            disconnectTasks.Add(Task.Run(() => session.Disconnect()));

        if (disconnectTasks.Count > 0)
            await Task.WhenAll(disconnectTasks);

        try { _serverSocket.Close(); } catch { /* Ignore */ }
    }

    public void Dispose()
    {
        _shutdownCts.Cancel();
        _shutdownCts.Dispose();

        try { _serverSocket.Close(); } catch { /* Ignore */ }

        foreach (var session in _sessions.Values)
            session.Dispose();
        _sessions.Clear();
    }
}