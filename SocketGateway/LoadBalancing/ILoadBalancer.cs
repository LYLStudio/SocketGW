using SocketGateway.Models;

namespace SocketGateway.LoadBalancing;

/// <summary>
/// 負載平衡演算法介面
/// </summary>
public interface ILoadBalancer
{
    /// <summary>
    /// 取得下一個目標 Server 節點
    /// </summary>
    /// <param name="availableNodes">當前可用的節點清單</param>
    /// <returns>選中的節點，如果沒有可用節點則回傳 null</returns>
    ServerNode? SelectNode(IEnumerable<ServerNode> availableNodes);

    /// <summary>
    /// 演算法名稱 (用於日誌/監控)
    /// </summary>
    string AlgorithmName { get; }
}