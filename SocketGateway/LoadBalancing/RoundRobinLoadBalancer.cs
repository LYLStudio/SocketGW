using SocketGateway.Models;

namespace SocketGateway.LoadBalancing;

/// <summary>
/// Round-Robin 負載平衡 — 輪循分配至各節點
/// Thread-safe via Interlocked
/// </summary>
public sealed class RoundRobinLoadBalancer : ILoadBalancer
{
    private long _index;

    public string AlgorithmName => "Round-Robin";

    public ServerNode? SelectNode(IEnumerable<ServerNode> availableNodes)
    {
        var nodes = availableNodes as ServerNode[] ?? availableNodes.ToArray();

        if (nodes.Length == 0) return null;
        if (nodes.Length == 1) return nodes[0];

        var idx = Math.Abs(Interlocked.Increment(ref _index) % nodes.Length);
        return nodes[idx];
    }
}