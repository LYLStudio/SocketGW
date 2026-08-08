namespace SocketGateway.Models;

/// <summary>
/// 後端 Server 節點狀態
/// </summary>
public enum NodeStatus
{
    Unknown,
    Healthy,
    Unhealthy,
    Draining,       // 正在逐出剩餘連線
    Disabled        // 手動停用
}

/// <summary>
/// 代表單一後端 Server 節點 (thread-safe)
/// </summary>
public sealed class ServerNode
{
    private readonly object _lock = new();
    private NodeStatus _status;
    private DateTime _lastHealthCheck;
    private DateTime _lastSuccessfulCheck;
    private int _consecutiveFailures;

    public string NodeId { get; }
    public string Host { get; }
    public int Port { get; }
    public string? Region { get; }
    public bool Enabled { get; private set; }

    public NodeStatus Status
    {
        get => _status;
        private set
        {
            lock (_lock) _status = value;
        }
    }

    private int _activeConnections;
    public int ActiveConnections => Volatile.Read(ref _activeConnections);
    public DateTime LastHealthCheck => _lastHealthCheck;
    public DateTime LastSuccessfulCheck => _lastSuccessfulCheck;
    public int ConsecutiveFailures => _consecutiveFailures;

    public bool IsAvailable => Status == NodeStatus.Healthy && Enabled;

    public ServerNode(string nodeId, string host, int port, string? region = null)
    {
        NodeId = nodeId;
        Host = host;
        Port = port;
        Region = region;
        Enabled = true;
        Status = NodeStatus.Unknown;
        _lastSuccessfulCheck = DateTime.UtcNow;
    }

    public void MarkHealthy()
    {
        lock (_lock)
        {
            Status = NodeStatus.Healthy;
            _lastSuccessfulCheck = DateTime.UtcNow;
            _consecutiveFailures = 0;
        }
    }

    public void MarkUnhealthy()
    {
        lock (_lock)
        {
            Status = NodeStatus.Unhealthy;
            _consecutiveFailures++;
        }
    }

    public void SetStatus(NodeStatus status)
    {
        lock (_lock)
        {
            Status = status;
            _lastHealthCheck = DateTime.UtcNow;
        }
    }

    public void Enable() => Enabled = true;
    public void Disable() => Enabled = false;

    public int AddConnection() => Interlocked.Increment(ref _activeConnections);
    public int RemoveConnection() => Math.Max(0, Interlocked.Decrement(ref _activeConnections));

    public override string ToString() => $"{NodeId} ({Host}:{Port}) [{Status}] conns={ActiveConnections}";
}