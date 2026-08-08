using SocketGateway.Models;

namespace SocketGateway.LoadBalancing;

/// <summary>
/// Least-Connections 負載平衡 — 選擇當前活躍連線數最少的節點
/// 適合長連線場景，自動避開高負載節點
/// </summary>
public sealed class LeastConnectionLoadBalancer : ILoadBalancer
{
    public string AlgorithmName => "Least-Connections";

    public ServerNode? SelectNode(IEnumerable<ServerNode> availableNodes)
    {
        var nodes = availableNodes as ServerNode[] ?? availableNodes.ToArray();

        if (nodes.Length == 0) return null;
        if (nodes.Length == 1) return nodes[0];

        ServerNode? selected = null;
        int minConnections = int.MaxValue;

        foreach (var node in nodes)
        {
            var conns = node.ActiveConnections;
            if (conns < minConnections)
            {
                minConnections = conns;
                selected = node;
            }
        }

        return selected;
    }
}