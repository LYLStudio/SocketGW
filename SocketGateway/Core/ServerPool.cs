using System.Collections.Concurrent;
using SocketGateway.Models;

namespace SocketGateway.Core;

/// <summary>
/// 管理所有後端 Server 節點的池
/// </summary>
public sealed class ServerPool
{
    private readonly ConcurrentDictionary<string, ServerNode> _nodes = new();
    private readonly object _lock = new();

    public event Action<ServerNode, NodeStatus>? NodeStatusChanged;

    /// <summary>
    /// 從配置批次加入 Server 節點
    /// </summary>
    public int AddNodes(IEnumerable<UpstreamServerConfig> configs)
    {
        var count = 0;
        foreach (var cfg in configs)
        {
            if (!cfg.TryValidate(out var error))
            {
                Console.WriteLine($"[Pool] Skip invalid config: {error}");
                continue;
            }

            lock (_lock)
            {
                if (!_nodes.ContainsKey(cfg.NodeId))
                {
                    var node = new ServerNode(cfg.NodeId, cfg.Host, cfg.Port, cfg.Region);
                    if (!cfg.Enabled) node.Disable();
                    _nodes[node.NodeId] = node;
                    count++;
                }
            }
        }
        return count;
    }

    /// <summary>
    /// 取得所有可用的節點 (Healthy + Enabled)
    /// </summary>
    public IEnumerable<ServerNode> GetAvailableNodes()
    {
        return _nodes.Values.Where(n => n.IsAvailable);
    }

    /// <summary>
    /// 依照 Region 篩選可用節點
    /// </summary>
    public IEnumerable<ServerNode> GetAvailableNodesInRegion(string region)
    {
        return _nodes.Values.Where(n => n.IsAvailable && string.Equals(n.Region, region, StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>
    /// 取得特定節點
    /// </summary>
    public ServerNode? GetNode(string nodeId)
    {
        _nodes.TryGetValue(nodeId, out var node);
        return node;
    }

    /// <summary>
    /// 移除節點 (先設定 Draining，等待連線歸零後再移除)
    /// </summary>
    public void RemoveNode(string nodeId)
    {
        lock (_lock)
        {
            if (_nodes.TryRemove(nodeId, out var node))
            {
                node.SetStatus(NodeStatus.Draining);
                node.Disable();
                NodeStatusChanged?.Invoke(node, NodeStatus.Draining);
            }
        }
    }

    /// <summary>
    /// 取得所有節點 (包含不可用)
    /// </summary>
    public IReadOnlyCollection<ServerNode> GetAllNodes()
    {
        return _nodes.Values.ToList().AsReadOnly();
    }

    /// <summary>
    /// 取得池狀態摘要
    /// </summary>
    public string GetPoolStatus()
    {
        var all = GetAllNodes();
        var healthy = all.Count(n => n.Status == NodeStatus.Healthy);
        var unhealthy = all.Count(n => n.Status == NodeStatus.Unhealthy);
        var totalConns = all.Sum(n => n.ActiveConnections);

        return $"Pool: {all.Count} nodes ({healthy} healthy, {unhealthy} unhealthy) | Total connections: {totalConns}";
    }

    internal void MarkNodeHealthy(string nodeId)
    {
        if (_nodes.TryGetValue(nodeId, out var node))
        {
            var prev = node.Status;
            node.MarkHealthy();
            if (prev != NodeStatus.Healthy)
                NodeStatusChanged?.Invoke(node, NodeStatus.Healthy);
        }
    }

    internal void MarkNodeUnhealthy(string nodeId)
    {
        if (_nodes.TryGetValue(nodeId, out var node))
        {
            var prev = node.Status;
            node.MarkUnhealthy();
            if (prev != NodeStatus.Unhealthy)
                NodeStatusChanged?.Invoke(node, NodeStatus.Unhealthy);
        }
    }
}