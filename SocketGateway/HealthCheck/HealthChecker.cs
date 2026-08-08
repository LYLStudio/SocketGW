using System.Collections.Concurrent;
using System.Net.Sockets;
using SocketGateway.Core;
using SocketGateway.Models;

namespace SocketGateway.HealthCheck;

/// <summary>
/// 定時對後端 Server 節點執行健康檢查
/// 機制: TCP connect 測試 + 自動標記不健康/恢復
/// </summary>
public sealed class HealthChecker : IDisposable
{
    private readonly ServerPool _pool;
    private readonly TimeSpan _interval;
    private readonly TimeSpan _timeout;
    private readonly int _unhealthyThreshold;
    private readonly int _healthyThreshold;

    private readonly CancellationTokenSource _cts = new();
    private Task? _runningTask;

    public event Action<ServerNode, NodeStatus>? NodeRecovered;
    public event Action<ServerNode, NodeStatus>? NodeFailed;

    // Per-node consecutive check tracking
    private readonly ConcurrentDictionary<string, int> _consecutiveHealthy = new();

    public HealthChecker(
        ServerPool pool,
        TimeSpan interval,
        TimeSpan timeout,
        int unhealthyThreshold,
        int healthyThreshold)
    {
        _pool = pool;
        _interval = interval;
        _timeout = timeout;
        _unhealthyThreshold = unhealthyThreshold;
        _healthyThreshold = healthyThreshold;
    }

    public void Start()
    {
        _runningTask = Task.Run(LoopAsync);
    }

    private async Task LoopAsync()
    {
        while (!_cts.Token.IsCancellationRequested)
        {
            try
            {
                await CheckAllNodesAsync();
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                Console.WriteLine($"[HealthChecker] Error: {ex.Message}");
            }

            await Task.Delay(_interval, _cts.Token);
        }
    }

    private async Task CheckAllNodesAsync()
    {
        var nodes = _pool.GetAllNodes();
        var tasks = new List<Task>();

        foreach (var node in nodes)
        {
            tasks.Add(CheckNodeAsync(node));
        }

        await Task.WhenAll(tasks);
    }

    private async Task CheckNodeAsync(ServerNode node)
    {
        var timeoutTokenSource = new CancellationTokenSource(_timeout);

        try
        {
            using var tcpClient = new TcpClient();
            var connectTask = tcpClient.ConnectAsync(node.Host, node.Port);

            // Race between connect and timeout
            var completedTask = await Task.WhenAny(connectTask, Task.Delay(_timeout, timeoutTokenSource.Token));

            if (completedTask == connectTask && tcpClient.Connected)
            {
                // Connection successful
                HandleHealthyCheck(node);
            }
            else
            {
                HandleUnhealthyCheck(node);
            }
        }
        catch (OperationCanceledException)
        {
            HandleUnhealthyCheck(node);
        }
        catch
        {
            HandleUnhealthyCheck(node);
        }
        finally
        {
            try { timeoutTokenSource.Cancel(); } catch { }
            timeoutTokenSource.Dispose();
        }
    }

    private void HandleHealthyCheck(ServerNode node)
    {
        // Track consecutive healthy checks for recovery
        _consecutiveHealthy.AddOrUpdate(node.NodeId, 1, (_, v) => v + 1);
        var count = _consecutiveHealthy[node.NodeId];

        if (node.Status == NodeStatus.Unhealthy && count >= _healthyThreshold)
        {
            // Recovered from unhealthy state
            _pool.MarkNodeHealthy(node.NodeId);
            _consecutiveHealthy[node.NodeId] = 0;
            NodeRecovered?.Invoke(node, NodeStatus.Healthy);
            Console.WriteLine($"[HealthCheck] NODE RECOVERED: {node}");
        }
        else if (node.Status == NodeStatus.Unknown)
        {
            // First time check - mark as healthy
            _pool.MarkNodeHealthy(node.NodeId);
            Console.WriteLine($"[HealthCheck] Node marked healthy on first check: {node}");
        }
    }

    private void HandleUnhealthyCheck(ServerNode node)
    {
        if (node.Status != NodeStatus.Unhealthy && node.Status != NodeStatus.Disabled)
        {
            _pool.MarkNodeUnhealthy(node.NodeId);
            NodeFailed?.Invoke(node, NodeStatus.Unhealthy);
            Console.WriteLine($"[HealthCheck] NODE FAILED: {node} (consecutive={node.ConsecutiveFailures})");
        }

        // Reset healthy counter on failure
        _consecutiveHealthy[node.NodeId] = 0;
    }

    /// <summary>
    /// Manual health check trigger for a specific node
    /// </summary>
    public async Task<bool> CheckNodeManualAsync(ServerNode node)
    {
        using var timeoutTokenSource = new CancellationTokenSource(_timeout);
        try
        {
            using var tcpClient = new TcpClient();
            await tcpClient.ConnectAsync(node.Host, node.Port, timeoutTokenSource.Token);
            return tcpClient.Connected;
        }
        catch
        {
            return false;
        }
    }

    public void Dispose()
    {
        _cts.Cancel();
        _cts.Dispose();
    }
}