using SocketServerLib;

Console.Title = "High Performance Socket Server";

await RunServerAsync();

static async Task RunServerAsync()
{
    // ===== 伺服器配置 =====
    var port = int.TryParse(Environment.GetEnvironmentVariable("SOCKET_SERVER_PORT"), out var p) ? p : 5000;
    var maxConnections = int.TryParse(Environment.GetEnvironmentVariable("MAX_CONNECTIONS"), out var m) ? m : 100_000;
    var backlog = int.TryParse(Environment.GetEnvironmentVariable("LISTEN_BACKLOG"), out var b) ? b : 10_000;
    var dualMode = Environment.GetEnvironmentVariable("DUAL_MODE") == "true";

    var separator = new string('=', 60);

    Console.WriteLine(separator);
    Console.WriteLine("  High-Performance Socket Server (.NET 10)");
    Console.WriteLine(separator);
    Console.WriteLine("  Config:");
    Console.WriteLine($"    Port...........: {port}");
    Console.WriteLine($"    Max Connections..: {maxConnections:N0}");
    Console.WriteLine($"    Listen Backlog...: {backlog:N0}");
    Console.WriteLine($"    Dual Mode (IPv6): {dualMode}");
    Console.WriteLine(separator);
    Console.WriteLine();

    var server = new HighPerformanceSocketServer(
        port: port,
        backlog: backlog,
        receiveBufferSize: 64 * 1024,
        sendBufferSize: 64 * 1024,
        dualMode: dualMode,
        maxConnections: maxConnections);

    // 訂閱連線事件
    server.ClientConnected += id => Console.WriteLine($"[+] Client connected: {id} (Active: {server.GetStatistics().CurrentConnections})");
    server.ClientDisconnected += id => Console.WriteLine($"[-] Client disconnected: {id} (Active: {server.GetStatistics().CurrentConnections})");

    // ===== 統計資訊報表 =====
    var statsTimer = StartStatsReporter(server);

    try
    {
        await server.StartAsync();
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[Fatal] Server failed to start: {ex.Message}");
    }
    finally
    {
        statsTimer.Dispose();
        server.Dispose();
    }
}

static Timer StartStatsReporter(HighPerformanceSocketServer server)
{
    return new Timer(_ =>
    {
        try
        {
            var stats = server.GetStatistics();
            Console.WriteLine(
                $"[Stats] Connections: {stats.CurrentConnections,5} | " +
                $"Total: {stats.TotalConnections,8:N0} | " +
                $"RX: {FormatBytes(stats.TotalBytesReceived),12} | " +
                $"TX: {FormatBytes(stats.TotalBytesSent),12}");
        }
        catch { /* Ignore stats errors */ }
    }, null, 5_000, 5_000);
}

static string FormatBytes(long bytes) => bytes switch
{
    < 0 => "N/A",
    < 1024 => $"{bytes} B",
    < 1024 * 1024 => $"{bytes / 1024.0:F2} KB",
    < 1024 * 1024 * 1024 => $"{bytes / (1024.0 * 1024):F2} MB",
    _ => $"{bytes / (1024.0 * 1024 * 1024):F2} GB"
};