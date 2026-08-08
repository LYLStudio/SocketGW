using System.Net.Sockets;
using System.Text;

namespace SocketClientLib;

/// <summary>
/// TCP Socket Client — 支援短連線指令與長連線互動模式
/// </summary>
public sealed class TcpSocketClient : IDisposable
{
    private TcpClient? _tcpClient;
    private bool _disposed;

    public string Host { get; }
    public int Port { get; }
    public bool IsConnected => _tcpClient?.Connected ?? false;

    // Events
    public event Action<string>? DataReceived;
    public event Action? Connected;
    public event Action? Disconnected;

    public TcpSocketClient(string host = "127.0.0.1", int port = 5000)
    {
        Host = host;
        Port = port;
    }

    /// <summary>
    /// 非同步連線至伺服器
    /// </summary>
    public async Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        _tcpClient = new TcpClient();
        await _tcpClient.ConnectAsync(Host, Port, cancellationToken);
        Connected?.Invoke();

        // 啟動背景接收任務
        _ = Task.Run(() => ReceiveLoopAsync(), cancellationToken);
    }

    /// <summary>
    /// 發送字串並等待回應 (短連線模式)
    /// </summary>
    public async Task<string> SendCommandAsync(string command, TimeSpan? timeout = null, CancellationToken ct = default)
    {
        var stream = _tcpClient?.GetStream() ?? throw new InvalidOperationException("Not connected");

        var data = Encoding.UTF8.GetBytes(command);
        await stream.WriteAsync(data, ct);

        // 接收回應
        var responseBuffer = new byte[65536];
        var totalBytes = 0;

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(
            ct, 
            new CancellationTokenSource(timeout ?? TimeSpan.FromSeconds(5)).Token);

        try
        {
            while (totalBytes < responseBuffer.Length && !cts.Token.IsCancellationRequested)
            {
                if (!stream.DataAvailable)
                {
                    await Task.Delay(10, cts.Token);
                    continue;
                }

                var bytesRead = await stream.ReadAsync(responseBuffer, totalBytes, 
                    responseBuffer.Length - totalBytes, cts.Token);

                if (bytesRead == 0) break;
                totalBytes += bytesRead;
            }
        }
        catch (OperationCanceledException) { /* Timeout */ }

        return Encoding.UTF8.GetString(responseBuffer, 0, totalBytes);
    }

    /// <summary>
    /// 發送 byte[] 資料
    /// </summary>
    public async Task SendAsync(byte[] data, CancellationToken ct = default)
    {
        var stream = _tcpClient?.GetStream() ?? throw new InvalidOperationException("Not connected");
        await stream.WriteAsync(data, ct);
    }

    /// <summary>
    /// 發送字串資料
    /// </summary>
    public async Task SendTextAsync(string text, CancellationToken ct = default)
    {
        await SendAsync(Encoding.UTF8.GetBytes(text), ct);
    }

    /// <summary>
    /// 背景接收循環
    /// </summary>
    private async Task ReceiveLoopAsync()
    {
        var buffer = new byte[8192];

        try
        {
            while (IsConnected && !_disposed)
            {
                var stream = _tcpClient?.GetStream();
                if (stream == null) break;

                if (!stream.DataAvailable)
                {
                    await Task.Delay(50);
                    continue;
                }

                var bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length);
                if (bytesRead == 0)
                {
                    Disconnected?.Invoke();
                    break;
                }

                var text = Encoding.UTF8.GetString(buffer, 0, bytesRead);
                DataReceived?.Invoke(text);
            }
        }
        catch { /* Connection closed */ }
        finally
        {
            Disconnected?.Invoke();
        }
    }

    /// <summary>
    /// 斷開連線
    /// </summary>
    public void Disconnect()
    {
        try { _tcpClient?.Close(); } catch { /* Ignore */ }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        try { _tcpClient?.Close(); } catch { /* Ignore */ }
        _tcpClient = null;
    }
}