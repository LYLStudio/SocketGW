using System.Buffers;
using System.Net.Sockets;

namespace SocketGateway.Core;

/// <summary>
/// 雙向資料透傳 — 在 Client Socket 和 Upstream Server Socket 之間建立雙向通道
/// </summary>
public static class ConnectionRelay
{
    private const int BufferSize = 8192;

    /// <summary>
    /// 啟動雙向資料中繼
    /// </summary>
    public static async Task RelayAsync(
        Socket clientSocket,
        Socket serverSocket,
        CancellationToken cancellationToken)
    {
        var clientToServer = PipeAsync(clientSocket, serverSocket, "[C→S]", cancellationToken);
        var serverToClient = PipeAsync(serverSocket, clientSocket, "[S→C]", cancellationToken);

        await Task.WhenAll(clientToServer, serverToClient);
    }

    private static async Task PipeAsync(
        Socket source,
        Socket destination,
        string label,
        CancellationToken cancellationToken)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(BufferSize);

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                // 使用 ReceiveAsync 讀取來源資料
                int bytesRead;
                try
                {
                    bytesRead = await Task.Run(
                        () => source.Receive(buffer, 0, BufferSize, SocketFlags.None),
                        cancellationToken);
                }
                catch (OperationCanceledException) { break; }
                catch { break; } // Socket closed

                if (bytesRead == 0) break; // 來源已關閉

                // 寫入目的地
                try
                {
                    await Task.Run(
                        () => destination.Send(buffer, 0, bytesRead, SocketFlags.None),
                        cancellationToken);
                }
                catch { break; } // 目的地已關閉
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    /// <summary>
    /// 使用 NetworkStream 的雙向中繼 (適合 TcpClient/TcpListener)
    /// </summary>
    public static async Task RelayStreamsAsync(
        NetworkStream clientStream,
        NetworkStream serverStream,
        CancellationToken cancellationToken)
    {
        var clientToServer = StreamPipeAsync(clientStream, serverStream, "[C→S]", cancellationToken);
        var serverToClient = StreamPipeAsync(serverStream, clientStream, "[S→C]", cancellationToken);

        await Task.WhenAll(clientToServer, serverToClient);
    }

    private static async Task StreamPipeAsync(
        Stream source,
        Stream destination,
        string label,
        CancellationToken cancellationToken)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(BufferSize);

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                int bytesRead;
                try
                {
                    bytesRead = await source.ReadAsync(buffer, 0, BufferSize, cancellationToken);
                }
                catch (OperationCanceledException) { break; }
                catch { break; }

                if (bytesRead == 0) break;

                try
                {
                    await destination.WriteAsync(buffer, 0, bytesRead, cancellationToken);
                    await destination.FlushAsync(cancellationToken);
                }
                catch { break; }
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }
}