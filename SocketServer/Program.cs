using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using System.Threading;
using SocketServerLib;

Console.Title = "High Performance Socket Server";

await RunServerAsync();

static async Task RunServerAsync()
{
    // ===== Load config from appsettings.json =====
    var config = LoadServerConfig();

    var separator = new string('=', 60);

    Console.WriteLine(separator);
    Console.WriteLine("  High-Performance Socket Server (.NET 10)");
    Console.WriteLine(separator);
    Console.WriteLine("  Config:");
    Console.WriteLine($"    Port...........: {config.Port}");
    Console.WriteLine($"    Max Connections..: {config.MaxConnections:N0}");
    Console.WriteLine($"    Listen Backlog...: {config.Backlog:N0}");
    Console.WriteLine($"    RX Buffer ......: {FormatBytes(config.ReceiveBufferSize)}");
    Console.WriteLine($"    TX Buffer ......: {FormatBytes(config.SendBufferSize)}");
    Console.WriteLine($"    Dual Mode (IPv6): {config.DualMode}");
    Console.WriteLine($"    Config Source ..: {(config.FromFile ? "appsettings.json" : "defaults/ENV")}");
    Console.WriteLine(separator);
    Console.WriteLine();

    var server = new HighPerformanceSocketServer(
        port: config.Port,
        backlog: config.Backlog,
        receiveBufferSize: config.ReceiveBufferSize,
        sendBufferSize: config.SendBufferSize,
        dualMode: config.DualMode,
        maxConnections: config.MaxConnections);

    // 訂閱連線事件
    server.ClientConnected += id => Console.WriteLine($"[+] Client connected: {id} (Active: {server.GetStatistics().CurrentConnections})");
    server.ClientDisconnected += id => Console.WriteLine($"[-] Client disconnected: {id} (Active: {server.GetStatistics().CurrentConnections})");

    // ===== 統計資訊報表 =====
    var statsTimer = StartStatsReporter(server, config.StatsIntervalSeconds);

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

static ServerConfig LoadServerConfig()
{
    var config = new ServerConfig();
    var configPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "appsettings.json");

    if (File.Exists(configPath))
    {
        try
        {
            var json = File.ReadAllText(configPath);
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;

            if (root.TryGetProperty("Server", out var srvElem))
            {
                config.Port = GetInt(srvElem, "Port", 5000);
                config.MaxConnections = GetInt(srvElem, "MaxConnections", 100_000);
                config.Backlog = GetInt(srvElem, "Backlog", 10_000);
                config.ReceiveBufferSize = GetInt(srvElem, "ReceiveBufferSize", 65_536);
                config.SendBufferSize = GetInt(srvElem, "SendBufferSize", 65_536);
                config.DualMode = GetBool(srvElem, "DualMode", false);
                config.StatsIntervalSeconds = GetInt(srvElem, "StatsIntervalSeconds", 5);
                config.FromFile = true;

                Console.WriteLine("[Server] Config loaded from appsettings.json");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Server] Config parse error: {ex.Message}. Using ENV/defaults.");
        }
    }

    // Environment variables override file config
    ApplyEnvOverrides(config);

    return config;
}

static void ApplyEnvOverrides(ServerConfig config)
{
    if (int.TryParse(Environment.GetEnvironmentVariable("SOCKET_SERVER_PORT"), out var p))
        config.Port = p;
    if (int.TryParse(Environment.GetEnvironmentVariable("MAX_CONNECTIONS"), out var m))
        config.MaxConnections = m;
    if (int.TryParse(Environment.GetEnvironmentVariable("LISTEN_BACKLOG"), out var b))
        config.Backlog = b;
    var dual = Environment.GetEnvironmentVariable("DUAL_MODE");
    if (dual != null)
        config.DualMode = dual.Equals("true", StringComparison.OrdinalIgnoreCase);
}

static int GetInt(JsonElement elem, string name, int @default)
{
    return elem.TryGetProperty(name, out var p) && p.ValueKind == JsonValueKind.Number ? p.GetInt32() : @default;
}

static bool GetBool(JsonElement elem, string name, bool @default)
{
    return elem.TryGetProperty(name, out var p) &&
           (p.ValueKind == JsonValueKind.True || p.ValueKind == JsonValueKind.False) ? p.GetBoolean() : @default;
}

static Timer StartStatsReporter(HighPerformanceSocketServer server, int intervalSec)
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
    }, null, intervalSec * 1000, intervalSec * 1000);
}

static string FormatBytes(long bytes) => bytes switch
{
    < 0 => "N/A",
    < 1024 => $"{bytes} B",
    < 1024 * 1024 => $"{bytes / 1024.0:F2} KB",
    < 1024 * 1024 * 1024 => $"{bytes / (1024.0 * 1024):F2} MB",
    _ => $"{bytes / (1024.0 * 1024 * 1024):F2} GB"
};

// ===== Config Model =====
sealed class ServerConfig
{
    public int Port { get; set; } = 5000;
    public int MaxConnections { get; set; } = 100_000;
    public int Backlog { get; set; } = 10_000;
    public int ReceiveBufferSize { get; set; } = 65_536;
    public int SendBufferSize { get; set; } = 65_536;
    public bool DualMode { get; set; } = false;
    public int StatsIntervalSeconds { get; set; } = 5;
    public bool FromFile { get; set; } = false;
}