using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;
using SocketGateway.Core;
using SocketGateway.HealthCheck;
using SocketGateway.LoadBalancing;
using SocketGateway.Models;

namespace SocketGateway;

/// <summary>
/// Gateway 主伺服器 — 同時監聽 TCP 和 WebSocket 連線
/// Client ──→ [Gateway] ──→ Upstream Server Pool
/// </summary>
public sealed class GatewayServer : IDisposable
{
    private readonly GatewayConfig _config;
    private readonly ServerPool _pool;
    private readonly ILoadBalancer _loadBalancer;
    private readonly GatewaySessionManager _sessionManager;
    private readonly HealthChecker _healthChecker;

    private TcpListener? _tcpListener;
    private TcpListener? _wsListener;
    private readonly CancellationTokenSource _shutdownCts = new();

    // Runtime stats
    private long _totalClientConnections = 0;
    private int _currentClientConnections = 0;
    private long _totalBytesRelayed = 0; // Updated by ConnectionRelay when byte tracking is enabled

    // Track active relay tasks for graceful shutdown
    private readonly ConcurrentDictionary<string, CancellationTokenSource> _activeRelays = new();

    public ServerPool Pool => _pool;
    public GatewaySessionManager SessionManager => _sessionManager;
    public bool IsRunning { get; private set; }

    public event Action<string>? ClientConnected;
    public event Action<string>? ClientDisconnected;

    public GatewayServer(GatewayConfig config)
    {
        _config = config;
        _pool = new ServerPool();
        _loadBalancer = CreateLoadBalancer(config.LoadBalanceAlgorithm);
        _sessionManager = new GatewaySessionManager(_pool, _loadBalancer, config.StickySession);
        _healthChecker = new HealthChecker(
            _pool,
            config.HealthCheckInterval,
            config.HealthCheckTimeout,
            config.UnhealthyThreshold,
            config.HealthyThreshold);

        // Wire up health check events to session reassignment
        _healthChecker.NodeFailed += (node, _) =>
        {
            Console.WriteLine($"[Gateway] Node {node.NodeId} failed - reassigning sessions");
            _sessionManager.ReassignFailedNode(node.NodeId);
        };
    }

    private static ILoadBalancer CreateLoadBalancer(string algorithm)
    {
        return algorithm.ToLowerInvariant() switch
        {
            "round-robin" or "rr" => new RoundRobinLoadBalancer(),
            "least-connections" or "lc" => new LeastConnectionLoadBalancer(),
            _ => new LeastConnectionLoadBalancer() // default
        };
    }

    public void Initialize(IEnumerable<UpstreamServerConfig> upstreamConfigs)
    {
        var added = _pool.AddNodes(upstreamConfigs);
        Console.WriteLine($"[Gateway] Added {added} upstream nodes to pool");
    }

    public async Task StartAsync()
    {
        IsRunning = true;

        // Start health checker
        _healthChecker.Start();
        Console.WriteLine("[Gateway] Health checker started");

        // Start TCP listener
        if (_config.TcpPort > 0)
        {
            _tcpListener = new TcpListener(IPAddress.Any, _config.TcpPort);
            _tcpListener.Start(_config.Backlog);
            _ = Task.Run(() => TcpAcceptLoopAsync());
            Console.WriteLine($"[Gateway] TCP listener started on port {_config.TcpPort}");
        }

        // Start WebSocket listener (reuses TCP with WS upgrade)
        if (_config.WebSocketPort > 0)
        {
            _wsListener = new TcpListener(IPAddress.Any, _config.WebSocketPort);
            _wsListener.Start(_config.Backlog);
            _ = Task.Run(() => WsAcceptLoopAsync());
            Console.WriteLine($"[Gateway] WebSocket listener started on port {_config.WebSocketPort}");
        }

        // Start stats reporter
        _ = Task.Run(() => StatsLoopAsync());

        Console.WriteLine("[Gateway] Gateway server started");
    }

    #region TCP Accept Loop

    private async Task TcpAcceptLoopAsync()
    {
        if (_tcpListener == null) return;

        while (!IsRunning && !_shutdownCts.Token.IsCancellationRequested) { } // wait until running

        while (!_shutdownCts.Token.IsCancellationRequested)
        {
            try
            {
                var client = await _tcpListener!.AcceptTcpClientAsync(_shutdownCts.Token);
                Interlocked.Increment(ref _totalClientConnections);
                Interlocked.Increment(ref _currentClientConnections);

                var clientKey = GetClientKey(client);
                ClientConnected?.Invoke(clientKey);

                _ = Task.Run(() => HandleTcpClientAsync(client, clientKey));
            }
            catch (OperationCanceledException) { break; }
            catch (ObjectDisposedException) { break; }
            catch (Exception ex)
            {
                Console.WriteLine($"[Gateway/TCP] Accept error: {ex.Message}");
            }
        }
    }

    private async Task HandleTcpClientAsync(TcpClient client, string clientKey)
    {
        ServerNode? assignedNode = null;
        TcpClient? upstreamClient = null;

        try
        {
            // Assign a backend server
            assignedNode = _sessionManager.AssignNode(clientKey);

            if (assignedNode == null)
            {
                await RejectClientAsync(client, "No available backend servers");
                return;
            }

            // Connect to upstream server
            upstreamClient = new TcpClient();
            await upstreamClient.ConnectAsync(assignedNode.Host, assignedNode.Port);

            var relayCts = new CancellationTokenSource();
            _activeRelays[clientKey] = relayCts;

            var clientStream = client.GetStream();
            var serverStream = upstreamClient.GetStream();

            // Start bidirectional relay
            await ConnectionRelay.RelayStreamsAsync(clientStream, serverStream, relayCts.Token);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Gateway/TCP] Relay error for {clientKey}: {ex.Message}");
        }
        finally
        {
            // Cleanup
            try { upstreamClient?.Close(); } catch { }
            try { client.Close(); } catch { }

            _activeRelays.TryRemove(clientKey, out _);
            _sessionManager.ReleaseSession(clientKey);
            Interlocked.Decrement(ref _currentClientConnections);
            ClientDisconnected?.Invoke(clientKey);
        }
    }

    #endregion

    #region WebSocket Accept Loop

    private async Task WsAcceptLoopAsync()
    {
        if (_wsListener == null) return;

        while (!_shutdownCts.Token.IsCancellationRequested)
        {
            try
            {
                var client = await _wsListener.AcceptTcpClientAsync(_shutdownCts.Token);
                Interlocked.Increment(ref _totalClientConnections);
                Interlocked.Increment(ref _currentClientConnections);

                var clientKey = GetClientKey(client);
                ClientConnected?.Invoke($"ws:{clientKey}");

                _ = Task.Run(() => HandleWebSocketClientAsync(client, clientKey));
            }
            catch (OperationCanceledException) { break; }
            catch (ObjectDisposedException) { break; }
            catch (Exception ex)
            {
                Console.WriteLine($"[Gateway/WS] Accept error: {ex.Message}");
            }
        }
    }

    private async Task HandleWebSocketClientAsync(TcpClient client, string clientKey)
    {
        ServerNode? assignedNode = null;
        TcpClient? upstreamClient = null;

        try
        {
            // Wait for HTTP upgrade request
            var stream = client.GetStream();
            var buffer = new byte[4096];

            int bytesRead = await stream.ReadAsync(buffer);
            var request = Encoding.UTF8.GetString(buffer, 0, bytesRead);

            if (!IsWebSocketUpgrade(request))
            {
                await SendHttpResponse(stream, 400, "Bad Request - Expected WebSocket upgrade");
                return;
            }

            // Assign backend & connect upstream
            assignedNode = _sessionManager.AssignNode($"ws:{clientKey}");
            if (assignedNode == null)
            {
                await SendHttpResponse(stream, 503, "No available backend servers");
                return;
            }

            upstreamClient = new TcpClient();
            await upstreamClient.ConnectAsync(assignedNode.Host, assignedNode.Port);

            // Send WebSocket upgrade response to client
            var upgradeResponse = "HTTP/1.1 101 Switching Protocols\r\n" +
                                  "Upgrade: websocket\r\n" +
                                  "Connection: Upgrade\r\n" +
                                  $"Sec-WebSocket-Accept: accepted\r\n" +
                                  "\r\n";
            await stream.WriteAsync(Encoding.UTF8.GetBytes(upgradeResponse));
            await stream.FlushAsync();

            var upstreamStream = upstreamClient.GetStream();
            var relayCts = new CancellationTokenSource();
            _activeRelays[$"ws:{clientKey}"] = relayCts;

            // Relay WebSocket frames (passthrough raw bytes after handshake)
            await ConnectionRelay.RelayStreamsAsync(stream, upstreamStream, relayCts.Token);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Gateway/WS] Relay error for ws:{clientKey}: {ex.Message}");
        }
        finally
        {
            try { upstreamClient?.Close(); } catch { }
            try { client.Close(); } catch { }

            _activeRelays.TryRemove($"ws:{clientKey}", out _);
            _sessionManager.ReleaseSession($"ws:{clientKey}");
            Interlocked.Decrement(ref _currentClientConnections);
            ClientDisconnected?.Invoke($"ws:{clientKey}");
        }
    }

    private static bool IsWebSocketUpgrade(string request)
    {
        return request.Contains("Upgrade", StringComparison.OrdinalIgnoreCase) &&
               request.Contains("websocket", StringComparison.OrdinalIgnoreCase);
    }

    private static async Task SendHttpResponse(Stream stream, int statusCode, string message)
    {
        var response = $"HTTP/1.1 {statusCode} {message}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
        await stream.WriteAsync(Encoding.UTF8.GetBytes(response));
        await stream.FlushAsync();
    }

    private static async Task RejectClientAsync(TcpClient client, string message)
    {
        try
        {
            var resp = Encoding.UTF8.GetBytes(message + "\n");
            await client.GetStream().WriteAsync(resp);
            await client.GetStream().FlushAsync();
        }
        catch { /* ignore */ }
    }

    #endregion

    #region Stats & Helpers

    private async Task StatsLoopAsync()
    {
        while (!_shutdownCts.Token.IsCancellationRequested)
        {
            try
            {
                var stats = GetStatistics();
                Console.WriteLine(
                    $"[GW] Clients: {stats.CurrentConnections,5} | " +
                    $"Total: {stats.TotalConnections,8:N0} | " +
                    $"Pool: {_pool.GetPoolStatus()} | " +
                    $"LB: {_loadBalancer.AlgorithmName}");
            }
            catch { /* ignore stats errors */ }

            await Task.Delay(_config.StatsInterval, _shutdownCts.Token);
        }
    }

    public GatewayStatistics GetStatistics()
    {
        return new GatewayStatistics
        {
            CurrentConnections = Volatile.Read(ref _currentClientConnections),
            TotalConnections = Volatile.Read(ref _totalClientConnections),
            BytesRelayed = Volatile.Read(ref _totalBytesRelayed),
            ActiveRelays = _activeRelays.Count,
            IsRunning = IsRunning
        };
    }

    private static string GetClientKey(TcpClient client)
    {
        var ep = client.Client.RemoteEndPoint as IPEndPoint;
        return $"{ep?.Address}:{ep?.Port}";
    }

    #endregion

    #region Shutdown & Dispose

    public async Task StopAsync()
    {
        if (!IsRunning) return;
        IsRunning = false;

        Console.WriteLine("[Gateway] Shutting down...");

        // Cancel all active relays
        foreach (var cts in _activeRelays.Values)
            cts.Cancel();

        _shutdownCts.Cancel();
        _healthChecker.Dispose();

        try { _tcpListener?.Stop(); } catch { }
        try { _wsListener?.Stop(); } catch { }

        Console.WriteLine("[Gateway] Shutdown complete");
    }

    public void Dispose()
    {
        _shutdownCts.Cancel();
        _shutdownCts.Dispose();
        _healthChecker.Dispose();

        try { _tcpListener?.Stop(); } catch { }
        try { _wsListener?.Stop(); } catch { }

        foreach (var cts in _activeRelays.Values)
            cts.Dispose();
        _activeRelays.Clear();

        _sessionManager.Clear();
    }

    #endregion
}

/// <summary>
/// Gateway 統計資料
/// </summary>
public sealed class GatewayStatistics
{
    public int CurrentConnections { get; init; }
    public long TotalConnections { get; init; }
    public long BytesRelayed { get; init; }
    public int ActiveRelays { get; init; }
    public bool IsRunning { get; init; }
}