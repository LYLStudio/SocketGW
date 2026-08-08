using System.Collections.Concurrent;
using System.Net;
using SocketGateway.LoadBalancing;
using SocketGateway.Models;

namespace SocketGateway.Core;

/// <summary>
/// 追蹤 Client ↔ Server 的對應關係，支援 Sticky Session
/// </summary>
public sealed class GatewaySessionManager
{
    private readonly ServerPool _pool;
    private readonly ILoadBalancer _loadBalancer;
    private readonly bool _stickySession;

    // client key (IP:Port or connection id) → assigned NodeId
    private readonly ConcurrentDictionary<string, string> _clientNodeMap = new();
    // nodeId → list of assigned client keys (for reassignment on failure)
    private readonly ConcurrentDictionary<string, ConcurrentBag<string>> _nodeClients = new();

    public int ActiveMappings => _clientNodeMap.Count;

    public GatewaySessionManager(
        ServerPool pool,
        ILoadBalancer loadBalancer,
        bool stickySession)
    {
        _pool = pool;
        _loadBalancer = loadBalancer;
        _stickySession = stickySession;
    }

    /// <summary>
    /// 為新 Client 分配後端 Server 節點
    /// - 若 Sticky Session 開啟且有既有映射 → 檢查節點是否仍存活，是則返回，否則重新分配
    /// - 否則透過 Load Balancer 選擇新節點
    /// </summary>
    public ServerNode? AssignNode(string clientKey)
    {
        if (_stickySession && _clientNodeMap.TryGetValue(clientKey, out var cachedNodeId))
        {
            // Check if cached node is still healthy
            var cachedNode = _pool.GetNode(cachedNodeId);
            if (cachedNode != null && cachedNode.IsAvailable)
            {
                return cachedNode;
            }

            // Node unavailable - remove stale mapping and reassign
            _clientNodeMap.TryRemove(clientKey, out _);
        }

        // Select a node via load balancer
        var availableNodes = _pool.GetAvailableNodes().ToList();
        var selected = _loadBalancer.SelectNode(availableNodes);

        if (selected == null) return null;

        // Record the mapping
        _clientNodeMap[clientKey] = selected.NodeId;
        _nodeClients.AddOrUpdate(selected.NodeId,
            _ => new ConcurrentBag<string> { clientKey },
            (_, bag) =>
            {
                bag.Add(clientKey);
                return bag;
            });

        selected.AddConnection();
        return selected;
    }

    /// <summary>
    /// Client 斷線時解除映射
    /// </summary>
    public void ReleaseSession(string clientKey)
    {
        if (_clientNodeMap.TryRemove(clientKey, out var nodeId))
        {
            var node = _pool.GetNode(nodeId);
            node?.RemoveConnection();

            // Remove from node's client list
            if (_nodeClients.TryGetValue(nodeId, out var clients))
                clients.TryTake(out _);
        }
    }

    /// <summary>
    /// 當節點變得不健康時，將其下所有 Client 映射標記為需要重新分配
    /// </summary>
    public void ReassignFailedNode(string nodeId)
    {
        if (!_nodeClients.TryRemove(nodeId, out var clientKeys)) return;

        foreach (var key in clientKeys)
        {
            _clientNodeMap.TryRemove(key, out _);
        }
    }

    /// <summary>
    /// 清除逾期的映射（可定時呼叫）
    /// </summary>
    public int ClearStaleMappings()
    {
        var count = 0;
        foreach (var kvp in _clientNodeMap.ToList())
        {
            var node = _pool.GetNode(kvp.Value);
            if (node == null || !node.IsAvailable)
            {
                _clientNodeMap.TryRemove(kvp.Key, out _);
                count++;
            }
        }
        return count;
    }

    public void Clear()
    {
        _clientNodeMap.Clear();
        _nodeClients.Clear();
    }

    /// <summary>
    /// Get routing info for monitoring
    /// </summary>
    public string GetRoutingStats()
    {
        var stats = new System.Text.StringBuilder();
        stats.AppendLine($"Sticky Session: {_stickySession}");
        stats.AppendLine($"Active Mappings: {ActiveMappings}");

        foreach (var kvp in _nodeClients)
        {
            var node = _pool.GetNode(kvp.Key);
            var clientCount = kvp.Value.Count;
            stats.AppendLine($"  {kvp.Key}: {clientCount} clients{(node != null ? $" [{node.Status}]" : " [MISSING]")}");
        }

        return stats.ToString();
    }
}