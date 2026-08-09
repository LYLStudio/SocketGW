using System.Net.Sockets;

namespace SocketGateway.Middleware;

/// <summary>
/// Relay middleware interface — optional pipeline stages injected into ConnectionRelay.
/// Each middleware can inspect/modify data flowing between Client ↔ Server.
/// </summary>
public interface IRelayMiddleware
{
    /// <summary>Human-readable name for logging/config.</summary>
    string Name { get; }

    /// <summary>
    /// Process a relay direction (source → destination).
    /// Called once per pipe direction when the pipe is established.
    /// </summary>
    Task<IRelayPipe?> CreatePipeAsync(RelayContext context);
}

/// <summary>
/// Context passed to middleware.
/// </summary>
public sealed class RelayContext
{
    public Socket SourceSocket { get; init; } = default!;
    public Socket DestinationSocket { get; init; } = default!;
    /// <summary>"C→S" or "S→C"</summary>
    public string Direction { get; init; } = "";
    /// <summary>Assigned backend NodeId</summary>
    public string? AssignedNodeId { get; set; }
    /// <summary>Arbitrary metadata for middleware communication.</summary>
    public Dictionary<string, object?> Metadata { get; } = new();
}

/// <summary>
/// A relay pipe created by middleware. Wraps the source socket with optional filtering/validation.
/// </summary>
public interface IRelayPipe : IDisposable
{
    /// <summary>Read from source (middleware can intercept).</summary>
    ValueTask<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken ct);
    /// <summary>Write to destination (middleware can intercept).</summary>
    ValueTask WriteAsync(byte[] buffer, int offset, int count, CancellationToken ct);
}

/// <summary>
/// Statistics collected by middleware during relay.
/// </summary>
public sealed class RelayStatistics
{
    public long BytesRelayed;
    public long ChunksProcessed;
    public long ValidationErrors;
    public string? LastNodeId;
}