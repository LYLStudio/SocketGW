using System.Diagnostics.CodeAnalysis;

namespace SocketGateway.Models;

/// <summary>
/// Gateway 配置模型
/// </summary>
public sealed class GatewayConfig
{
    // Gateway 自身監聽設定
    public int TcpPort { get; set; } = 8080;
    public int WebSocketPort { get; set; } = 8081;
    public int Backlog { get; set; } = 10_000;

    // 健康檢查
    public TimeSpan HealthCheckInterval { get; set; } = TimeSpan.FromSeconds(5);
    public TimeSpan HealthCheckTimeout { get; set; } = TimeSpan.FromSeconds(2);
    public int UnhealthyThreshold { get; set; } = 3;
    public int HealthyThreshold { get; set; } = 2;

    // Session 親和性
    public bool StickySession { get; set; } = true;
    public TimeSpan SessionTimeout { get; set; } = TimeSpan.FromMinutes(30);

    // 負載平衡
    public string LoadBalanceAlgorithm { get; set; } = "least-connections";

    // 後端 Server 清單
    public List<UpstreamServerConfig> Upstreams { get; set; } = new();

    // 日誌
    public bool VerboseLogging { get; set; } = false;
    public TimeSpan StatsInterval { get; set; } = TimeSpan.FromSeconds(5);
}

/// <summary>
/// 單一後端 Server 配置
/// </summary>
public sealed class UpstreamServerConfig
{
    public string NodeId { get; set; } = string.Empty;
    public string Host { get; set; } = "127.0.0.1";
    public int Port { get; set; }
    public string? Region { get; set; }
    public bool Enabled { get; set; } = true;

    public bool TryValidate([NotNullWhen(true)] out string? error)
    {
        var result = true;
        error = null;
        if (string.IsNullOrWhiteSpace(NodeId))
        {
            error = "NodeId is required";
            return false;
        }
        if (string.IsNullOrWhiteSpace(Host))
        {
            error = "Host is required";
            return false;
        }
        if (Port <= 0 || Port > 65535)
        {
            error = "Port must be between 1 and 65535";
            return false;
        }
        return result;
    }
}