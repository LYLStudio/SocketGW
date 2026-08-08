using SocketGateway;
using SocketGateway.Models;

Console.Title = "Socket Gateway";

await RunGatewayAsync();

static async Task RunGatewayAsync()
{
    // ===== Load configuration =====
    var config = LoadConfig();

    var separator = new string('=', 70);

    Console.WriteLine(separator);
    Console.WriteLine("  Socket Gateway Server");
    Console.WriteLine(separator);
    Console.WriteLine($"  TCP Port ........: {config.TcpPort}");
    Console.WriteLine($"  WebSocket Port ..: {config.WebSocketPort}");
    Console.WriteLine($"  LB Algorithm ....: {config.LoadBalanceAlgorithm}");
    Console.WriteLine($"  Sticky Session ..: {config.StickySession}");
    Console.WriteLine($"  Health Check ....: every {config.HealthCheckInterval.TotalSeconds}s");
    Console.WriteLine($"  Upstream Nodes ..: {config.Upstreams.Count}");

    foreach (var upstream in config.Upstreams)
        Console.WriteLine($"    - {upstream.NodeId}: {upstream.Host}:{upstream.Port} [{upstream.Region ?? "default"}] {(upstream.Enabled ? "[ON]" : "[OFF]")}");

    Console.WriteLine(separator);
    Console.WriteLine();

    // ===== Create & start Gateway =====
    using var gateway = new GatewayServer(config);
    gateway.Initialize(config.Upstreams);

    // Subscribe to events
    gateway.ClientConnected += key =>
    {
        if (config.VerboseLogging)
            Console.WriteLine($"[+] Client connected: {key}");
    };
    gateway.ClientDisconnected += key =>
    {
        if (config.VerboseLogging)
            Console.WriteLine($"[-] Client disconnected: {key}");
    };

    // Start with Ctrl+C handling
    var cts = new CancellationTokenSource();
    Console.CancelKeyPress += (_, e) =>
    {
        e.Cancel = true;
        cts.Cancel();
    };

    try
    {
        await gateway.StartAsync();

        // Wait until shutdown signal
        while (!cts.Token.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(1000, cts.Token);
            }
            catch (TaskCanceledException) { break; }
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[Gateway] Fatal: {ex.Message}");
    }
    finally
    {
        await gateway.StopAsync();
    }
}

static GatewayConfig LoadConfig()
{
    var config = new GatewayConfig();

    // Try loading from appsettings.json
    var configPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "appsettings.json");

    if (File.Exists(configPath))
    {
        try
        {
            var json = File.ReadAllText(configPath);
            var doc = System.Text.Json.JsonDocument.Parse(json);
            var root = doc.RootElement;

            // Parse Gateway settings
            if (root.TryGetProperty("Gateway", out var gwElem))
            {
                config.TcpPort = gwElem.GetIntPropertyOrDefault("TcpPort", config.TcpPort);
                config.WebSocketPort = gwElem.GetIntPropertyOrDefault("WebSocketPort", config.WebSocketPort);
                config.Backlog = gwElem.GetIntPropertyOrDefault("Backlog", config.Backlog);
                config.LoadBalanceAlgorithm = gwElem.GetStringPropertyOrDefault("LoadBalanceAlgorithm", config.LoadBalanceAlgorithm);
                config.StickySession = gwElem.GetBooleanPropertyOrDefault("StickySession", config.StickySession);
                config.VerboseLogging = gwElem.GetBooleanPropertyOrDefault("VerboseLogging", config.VerboseLogging);

                var hcInterval = gwElem.GetIntPropertyOrDefault("HealthCheckIntervalSeconds", (int)config.HealthCheckInterval.TotalSeconds);
                config.HealthCheckInterval = TimeSpan.FromSeconds(hcInterval);

                var hcTimeout = gwElem.GetIntPropertyOrDefault("HealthCheckTimeoutSeconds", (int)config.HealthCheckTimeout.TotalSeconds);
                config.HealthCheckTimeout = TimeSpan.FromSeconds(hcTimeout);

                config.UnhealthyThreshold = gwElem.GetIntPropertyOrDefault("UnhealthyThreshold", config.UnhealthyThreshold);
                config.HealthyThreshold = gwElem.GetIntPropertyOrDefault("HealthyThreshold", config.HealthyThreshold);

                var statsInterval = gwElem.GetIntPropertyOrDefault("StatsIntervalSeconds", (int)config.StatsInterval.TotalSeconds);
                config.StatsInterval = TimeSpan.FromSeconds(statsInterval);
            }

            // Parse Upstreams
            if (root.TryGetProperty("Upstreams", out var upstreamsElem))
            {
                foreach (var ue in upstreamsElem.EnumerateArray())
                {
                    var upstream = new UpstreamServerConfig();
                    upstream.NodeId = ue.GetStringPropertyOrDefault("NodeId", "unknown");
                    upstream.Host = ue.GetStringPropertyOrDefault("Host", "127.0.0.1");
                    upstream.Port = ue.GetIntPropertyOrDefault("Port", 5000);
                    upstream.Region = ue.TryGetProperty("Region", out var region) ? region.ToString() : null;
                    upstream.Enabled = ue.GetBooleanPropertyOrDefault("Enabled", true);
                    config.Upstreams.Add(upstream);
                }
            }

            Console.WriteLine("[Gateway] Config loaded from appsettings.json");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Gateway] Failed to load config: {ex.Message}. Using defaults.");
        }
    }
    else
    {
        // Fallback: default single upstream
        config.Upstreams.Add(new UpstreamServerConfig
        {
            NodeId = "default-server",
            Host = "127.0.0.1",
            Port = 5000,
            Enabled = true
        });

        Console.WriteLine("[Gateway] No appsettings.json found. Using default config (upstream: 127.0.0.1:5000)");
    }

    return config;
}

// Helper extension methods for JSON parsing
static class JsonPropertyExtensions
{
    public static int GetIntPropertyOrDefault(this System.Text.Json.JsonElement elem, string name, int @default)
    {
        return elem.TryGetProperty(name, out var p) && p.ValueKind == System.Text.Json.JsonValueKind.Number ? p.GetInt32() : @default;
    }

    public static string GetStringPropertyOrDefault(this System.Text.Json.JsonElement elem, string name, string @default)
    {
        return elem.TryGetProperty(name, out var p) && p.ValueKind == System.Text.Json.JsonValueKind.String ? p.GetString()! : @default;
    }

    public static bool GetBooleanPropertyOrDefault(this System.Text.Json.JsonElement elem, string name, bool @default)
    {
        return elem.TryGetProperty(name, out var p) && (p.ValueKind == System.Text.Json.JsonValueKind.True || p.ValueKind == System.Text.Json.JsonValueKind.False) ? p.GetBoolean() : @default;
    }
}