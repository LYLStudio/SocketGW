using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using SocketClientLib;

namespace SocketTests;

// ============================================================================
enum ClientType { ShortFast, HeavyTransfer, Bursty }

sealed class TestStats
{
    public ConcurrentBag<int> ConnectSuccess = new();
    public ConcurrentBag<double> ConnectTimes = new();
    public ConcurrentBag<int> PayloadSizes = new();
    public ConcurrentBag<double> Intervals = new();

    public void RecordConnect(double ms) { ConnectSuccess.Add(1); ConnectTimes.Add(ms); }
    public void RecordPayloadSize(int s) => PayloadSizes.Add(s);
    public void RecordInterval(double ms) => Intervals.Add(ms);
}

sealed class ClientResult
{
    public int Id { get; set; }
    public bool ConnectSuccess { get; set; }
    public string? ErrorMessage { get; set; }
    public double ConnectTimeMs { get; set; }
    public int MessagesSent { get; set; }
    public long BytesSent { get; set; }
    public long BytesReceived { get; set; }
    public int Errors { get; set; }
}

public sealed class Args
{
    public string Mode { get; set; } = "basic";
    public int NumClients { get; set; } = 100;
    public int DurationSec { get; set; } = 10;
    public string Host { get; set; } = "127.0.0.1";
    public int Port { get; set; } = 8080;
    public int BatchSize { get; set; } = 500;
    public int KillServerPort { get; set; } = 5002;

    public static Args Parse(string[] rawArgs)
    {
        var result = new Args();

        // First pass: determine mode & apply mode defaults
        for (int i = 0; i < rawArgs.Length; i++)
        {
            var a = rawArgs[i].Trim().ToLowerInvariant();
            if (a == "basic") { result.Mode = a; result.NumClients = 100; result.DurationSec = 10; result.Port = 5000; break; }
            else if (a == "advanced") { result.Mode = a; result.NumClients = 10_000; result.DurationSec = 30; result.Port = 8080; break; }
            else if (a == "resilience") { result.Mode = a; result.NumClients = 100_000; result.DurationSec = 45; result.Port = 8080; result.BatchSize = 2000; break; }
            else if (a == "all") { result.Mode = a; break; }
        }

        // Second pass: CLI overrides mode defaults
        for (int i = 0; i < rawArgs.Length; i++)
        {
            var a = rawArgs[i].Trim().ToLowerInvariant();
            switch (a)
            {
                case "basic": case "advanced": case "resilience": case "all": break; /* already handled */
                case "--clients": case "-c": result.NumClients = int.Parse(rawArgs[++i]); break;
                case "--duration": case "-d": result.DurationSec = int.Parse(rawArgs[++i]); break;
                case "--host": case "-h": result.Host = rawArgs[++i]; break;
                case "--port": case "-p": result.Port = int.Parse(rawArgs[++i]); break;
                case "--batch": case "-b": result.BatchSize = int.Parse(rawArgs[++i]); break;
                case "--kill-port": case "-k": result.KillServerPort = int.Parse(rawArgs[++i]); break;
            }
        }

        return result;
    }
}

// ============================================================================
class Program
{
    static ClientType DetermineClientType(int clientId, int totalClients)
    {
        double ratio = (double)(clientId % 100) / 100.0;
        if (ratio < 0.60) return ClientType.ShortFast;
        if (ratio < 0.85) return ClientType.HeavyTransfer;
        return ClientType.Bursty;
    }

    static byte[] GenPayload(int size, int clientId, int seq)
    {
        var header = $"MSG:{clientId}:{seq}:LEN={size}";
        var hBytes = Encoding.ASCII.GetBytes(header);
        var buf = new byte[size];
        Buffer.BlockCopy(hBytes, 0, buf, 0, Math.Min(hBytes.Length, size));
        for (int i = Math.Min(hBytes.Length, size); i < size; i++)
            buf[i] = (byte)((clientId + seq + i) % 256);
        return buf;
    }

    static double Percentile(List<double> vals, double pct)
    {
        if (vals.Count == 0) return 0;
        vals.Sort();
        int idx = Math.Max(0, Math.Min((int)Math.Ceiling(pct / 100.0 * vals.Count) - 1, vals.Count - 1));
        return vals[idx];
    }

    static string FormatBytes(long bytes)
    {
        if (bytes < 0) return "N/A";
        if (bytes < 1024) return $"{bytes} B";
        if (bytes < 1024 * 1024) return $"{bytes / 1024.0:F2} KB";
        if (bytes < 1024 * 1024 * 1024) return $"{bytes / (1024.0 * 1024):F2} MB";
        return $"{bytes / (1024.0 * 1024 * 1024):F2} GB";
    }

    // ===== Entry Point =====
    public static async Task Main(string[] args)
    {
        var cfg = Args.Parse(args);
        if (cfg.Mode == "all") await RunAllTestsSequentially(cfg);
        else await RunSingleTest(cfg);
    }

    static async Task RunSingleTest(Args cfg)
    {
        switch (cfg.Mode)
        {
            case "basic": await RunBasicTest(cfg); break;
            case "advanced": await RunAdvancedTest(cfg); break;
            case "resilience": await RunResilienceTest(cfg); break;
            default:
                Console.WriteLine($"Unknown test mode: {cfg.Mode}");
                Console.WriteLine("Use: basic | advanced | resilience | all");
                Environment.Exit(1); break;
        }
    }

    static async Task RunAllTestsSequentially(Args cfg)
    {
        var sep = new string('=', 80);
        Console.WriteLine(sep);
        Console.WriteLine("  RUNNING ALL TESTS SEQUENTIALLY");
        Console.WriteLine(sep);
        Console.WriteLine();

        Console.WriteLine("[>>>] Starting BASIC test...");
        await RunBasicTest(new Args { Mode = "basic", NumClients = cfg.NumClients, DurationSec = cfg.DurationSec, Host = cfg.Host, Port = cfg.Port });
        Console.WriteLine(); Console.WriteLine("[OK]  BASIC test completed.\n");

        Console.WriteLine("[>>>] Starting ADVANCED test...");
        await RunAdvancedTest(new Args { Mode = "advanced", NumClients = cfg.NumClients, DurationSec = cfg.DurationSec, Host = cfg.Host, Port = cfg.Port, BatchSize = cfg.BatchSize });
        Console.WriteLine(); Console.WriteLine("[OK]  ADVANCED test completed.\n");

        Console.WriteLine("[>>>] Starting RESILIENCE test...");
        await RunResilienceTest(new Args { Mode = "resilience", NumClients = Math.Min(cfg.NumClients, 1000), DurationSec = cfg.DurationSec, Host = cfg.Host, Port = cfg.Port, BatchSize = cfg.BatchSize, KillServerPort = cfg.KillServerPort });
        Console.WriteLine(); Console.WriteLine("[OK]  RESILIENCE test completed.\n");

        Console.WriteLine(sep);
        Console.WriteLine("  ALL TESTS COMPLETED");
        Console.WriteLine(sep);
    }

    // ========================================================================
    // BASIC TEST
    // ========================================================================

    static async Task RunBasicTest(Args cfg)
    {
        var sep = new string('=', 68);
        var line = new string('-', 68);

        Console.WriteLine(sep);
        Console.WriteLine("  BASIC LOAD TEST");
        Console.WriteLine(sep);
        Console.WriteLine($"  Clients .....: {cfg.NumClients}");
        Console.WriteLine($"  Duration ....: {cfg.DurationSec} seconds");
        Console.WriteLine($"  Target ......: {cfg.Host}:{cfg.Port}");
        Console.WriteLine(sep);
        Console.WriteLine();

        var sw = Stopwatch.StartNew();
        var results = new List<Task<ClientResult>>();
        Console.WriteLine($"[{sw.Elapsed.TotalSeconds:F2}s] Starting {cfg.NumClients} clients...");

        for (int i = 0; i < cfg.NumClients; i++)
            results.Add(BasicClientWorker(i, cfg));

        var allResults = await Task.WhenAll(results);
        sw.Stop();
        PrintBasicReport(allResults, cfg, sw.Elapsed.TotalSeconds, sep, line);
    }

    static async Task<ClientResult> BasicClientWorker(int id, Args cfg)
    {
        var result = new ClientResult { Id = id };
        try
        {
            using var client = new TcpSocketClient(cfg.Host, cfg.Port);
            var connSw = Stopwatch.StartNew();
            await Task.WhenAny(client.ConnectAsync(), Task.Delay(8000));
            connSw.Stop();

            if (!client.IsConnected) { result.ErrorMessage = "Connection timeout after 8s"; return result; }
            result.ConnectSuccess = true;
            result.ConnectTimeMs = connSw.Elapsed.TotalMilliseconds;

            long recvBytes = 0;
            client.DataReceived += text => Interlocked.Add(ref recvBytes, Encoding.UTF8.GetByteCount(text));

            var payload = $"Hello from Client #{id}! [message]";
            var payloadBytes = Encoding.UTF8.GetBytes(payload);
            int sendCount = 0, errCount = 0;
            long sentBytes = 0;
            var endTime = DateTime.UtcNow.AddSeconds(cfg.DurationSec);

            while (DateTime.UtcNow < endTime && client.IsConnected)
            {
                try
                {
                    await client.SendAsync(payloadBytes);
                    Interlocked.Add(ref sentBytes, payloadBytes.Length);
                    sendCount++;
                    if (sendCount % 100 == 0) await Task.Delay(1);
                } catch { errCount++; }
            }

            result.MessagesSent = sendCount;
            result.BytesSent = sentBytes;
            result.BytesReceived = recvBytes;
            result.Errors = errCount;
        }
        catch (Exception ex) { result.ErrorMessage = ex.Message; }
        return result;
    }

    static void PrintBasicReport(ClientResult[] results, Args cfg, double elapsed, string sep, string line)
    {
        var success = results.Where(r => r.ConnectSuccess).ToList();
        var failed = results.Where(r => !r.ConnectSuccess).ToList();

        Console.WriteLine();
        Console.WriteLine(sep);
        Console.WriteLine("  Basic Load Test Report");
        Console.WriteLine(sep);
        Console.WriteLine($"  Duration ..........: {elapsed:F2} s");
        Console.WriteLine(line);
        Console.WriteLine($"  Total Clients .....: {cfg.NumClients}");
        Console.WriteLine($"  Successful Connect : {success.Count}");
        Console.WriteLine($"  Failed Connect ....: {failed.Count}");
        Console.WriteLine(line);

        if (success.Count > 0)
        {
            var times = success.Select(r => r.ConnectTimeMs).ToList();
            Console.WriteLine("  Connection Latency:");
            Console.WriteLine($"    Min .......: {times.Min():F2} ms");
            Console.WriteLine($"    Max .......: {times.Max():F2} ms");
            Console.WriteLine($"    Avg .......: {times.Average():F2} ms");
            Console.WriteLine(line);

            var totalMsgs = success.Sum(r => r.MessagesSent);
            var sent = success.Sum(r => r.BytesSent);
            var recv = success.Sum(r => r.BytesReceived);
            var errs = success.Sum(r => r.Errors);
            var io = sent + recv;

            Console.WriteLine("  Data Transfer:");
            Console.WriteLine($"    Total Msgs ..: {totalMsgs:N0}");
            Console.WriteLine($"    Bytes Sent ..: {FormatBytes(sent)}");
            Console.WriteLine($"    Bytes Recv ..: {FormatBytes(recv)}");
            Console.WriteLine($"    Total I/O ...: {FormatBytes(io)}");
            Console.WriteLine($"    Throughput ..: {FormatBytes((long)(io / elapsed))}/s");
            Console.WriteLine(line);
            Console.WriteLine($"    Msg Rate ....: {totalMsgs / elapsed:F1} msg/s");
            Console.WriteLine($"    Errors ......: {errs}");
        }
        else Console.WriteLine("  No successful connections!");

        Console.WriteLine(sep);
        var passed = success.Count >= cfg.NumClients * 0.9;
        Console.WriteLine($"  Result: {(passed ? "[PASS]" : "[FAIL]")} {success.Count}/{cfg.NumClients} clients connected ({(double)success.Count / cfg.NumClients:P1})");
        Console.WriteLine(sep);

        if (failed.Count > 0)
        {
            var showCount = Math.Min(failed.Count, 15);
            Console.WriteLine($"\n  {failed.Count} clients failed (showing first {showCount}):");
            foreach (var f in failed.Take(showCount))
                Console.WriteLine($"    #{f.Id}: {f.ErrorMessage}");
        }
    }

    // ========================================================================
    // ADVANCED TEST
    // ========================================================================

    static async Task RunAdvancedTest(Args cfg)
    {
        var sep = new string('=', 72);
        var line = new string('-', 72);

        Console.WriteLine(sep);
        Console.WriteLine("  ADVANCED STRESS TEST (Variable Payload + Random Intervals + Client Types)");
        Console.WriteLine(sep);
        Console.WriteLine($"  Total Clients .: {cfg.NumClients:N0}");
        Console.WriteLine($"  Duration .....: {cfg.DurationSec} seconds");
        Console.WriteLine($"  Batch Size ...: {cfg.BatchSize} (staggered connect)");
        Console.WriteLine($"  Target .......: {cfg.Host}:{cfg.Port}");
        Console.WriteLine(line);
        Console.WriteLine($"  Type A (60%): short-fast clients — small payload, fast send");
        Console.WriteLine($"  Type B (25%): heavy-transfer clients — large payload, medium send");
        Console.WriteLine($"  Type C (15%): bursty clients — random bursts with idle gaps");
        Console.WriteLine(sep);
        Console.WriteLine();

        var stats = new TestStats();
        var sw = Stopwatch.StartNew();
        var totalBatches = (int)Math.Ceiling((double)cfg.NumClients / cfg.BatchSize);
        var tasks = new List<Task<ClientResult>>();

        for (int b = 0; b < totalBatches; b++)
        {
            int startId = b * cfg.BatchSize;
            int count = Math.Min(cfg.BatchSize, cfg.NumClients - startId);
            Console.WriteLine($"[{sw.Elapsed.TotalSeconds:F1}s] Batch {b + 1}/{totalBatches}: launching {count} clients (#{startId}..#{startId + count - 1})");

            for (int i = startId; i < startId + count; i++)
            {
                var type = DetermineClientType(i, cfg.NumClients);
                tasks.Add(AdvancedClientWorker(i, cfg, type, stats));
            }
            if (b < totalBatches - 1) await Task.Delay(50);
        }

        Console.WriteLine($"[{sw.Elapsed.TotalSeconds:F1}s] All clients launched. Waiting for completion...");
        var allResults = await Task.WhenAll(tasks);
        sw.Stop();
        PrintAdvancedReport(allResults, cfg, stats, sw.Elapsed.TotalSeconds, sep, line);
    }

    static async Task<ClientResult> AdvancedClientWorker(int id, Args cfg, ClientType type, TestStats stats)
    {
        var result = new ClientResult { Id = id };
        try
        {
            using var client = new TcpSocketClient(cfg.Host, cfg.Port);
            var connSw = Stopwatch.StartNew();
            await Task.WhenAny(client.ConnectAsync(), Task.Delay(8000));
            connSw.Stop();

            if (!client.IsConnected) { result.ErrorMessage = "Connection timeout after 8s"; return result; }
            result.ConnectSuccess = true;
            result.ConnectTimeMs = connSw.Elapsed.TotalMilliseconds;
            stats.RecordConnect(connSw.Elapsed.TotalMilliseconds);

            var rng = new Random(id * 31 + 7);
            long recvBytes = 0, sentBytes = 0;
            int msgCount = 0, errCount = 0;

            client.DataReceived += text => Interlocked.Add(ref recvBytes, Encoding.UTF8.GetByteCount(text));
            var endTime = DateTime.UtcNow.AddSeconds(cfg.DurationSec);

            while (DateTime.UtcNow < endTime && client.IsConnected)
            {
                try
                {
                    byte[] payload; int delayMs;

                    if (type == ClientType.ShortFast)
                    { payload = GenPayload(64 + rng.Next(0, 192), id, msgCount); delayMs = 10 + rng.Next(0, 20); }
                    else if (type == ClientType.HeavyTransfer)
                    { payload = GenPayload(512 + rng.Next(0, 3584), id, msgCount); delayMs = 50 + rng.Next(0, 100); }
                    else if (rng.Next(0, 3) == 0)
                    {
                        /* Burst mode */
                        for (int burst = 0; burst < rng.Next(5, 16); burst++)
                        {
                            payload = GenPayload(64 + rng.Next(0, 2048), id, msgCount);
                            await client.SendAsync(payload);
                            Interlocked.Add(ref sentBytes, payload.Length);
                            stats.RecordPayloadSize(payload.Length);
                            msgCount++;
                            stats.RecordInterval(rng.Next(1, 5));
                            await Task.Delay(rng.Next(1, 5));
                        }
                        delayMs = 200 + rng.Next(0, 500);
                        payload = GenPayload(0, id, msgCount); /* dummy to satisfy compiler */
                    }
                    else
                    { payload = GenPayload(128 + rng.Next(0, 1024), id, msgCount); delayMs = 30 + rng.Next(0, 200); }

                    if (type != ClientType.Bursty || rng.Next(0, 3) != 0)
                    { /* handled */ }
                    else
                    { await client.SendAsync(payload); Interlocked.Add(ref sentBytes, payload.Length); stats.RecordPayloadSize(payload.Length); msgCount++; }

                    stats.RecordInterval(delayMs);
                    await Task.Delay(delayMs);
                } catch { errCount++; }
            }

            result.MessagesSent = msgCount;
            result.BytesSent = sentBytes;
            result.BytesReceived = recvBytes;
            result.Errors = errCount;
        }
        catch (Exception ex) { result.ErrorMessage = ex.Message; }
        return result;
    }

    static void PrintAdvancedReport(ClientResult[] results, Args cfg, TestStats stats, double elapsed, string sep, string line)
    {
        var success = results.Where(r => r.ConnectSuccess).ToList();
        var failed = results.Where(r => !r.ConnectSuccess).ToList();
        var totalMsgs = success.Sum(r => r.MessagesSent);
        var sent = success.Sum(r => r.BytesSent);
        var recv = success.Sum(r => r.BytesReceived);
        var errs = success.Sum(r => r.Errors);
        var io = sent + recv;

        Console.WriteLine();
        Console.WriteLine(sep);
        Console.WriteLine("  Advanced Stress Test Report");
        Console.WriteLine(sep);
        Console.WriteLine($"  Duration ..........: {elapsed:F2} s");
        Console.WriteLine(line);
        Console.WriteLine($"  Total Clients .....: {cfg.NumClients:N0}");
        Console.WriteLine($"  Successful Connect : {success.Count:N0}");
        Console.WriteLine($"  Failed Connect ....: {failed.Count:N0}");
        Console.WriteLine(line);

        if (stats.ConnectTimes.Count > 0)
        {
            var times = stats.ConnectTimes.ToList();
            Console.WriteLine("  Connection Latency:");
            Console.WriteLine($"    Avg .......: {times.Average():F2} ms");
            Console.WriteLine($"    P50 .......: {Percentile(times, 50):F2} ms");
            Console.WriteLine($"    P95 .......: {Percentile(times, 95):F2} ms");
            Console.WriteLine($"    P99 .......: {Percentile(times, 99):F2} ms");
            Console.WriteLine(line);
        }

        Console.WriteLine("  Data Transfer:");
        Console.WriteLine($"    Total Msgs ....: {totalMsgs:N0}");
        Console.WriteLine($"    Bytes Sent ....: {FormatBytes(sent)}");
        Console.WriteLine($"    Bytes Received : {FormatBytes(recv)}");
        Console.WriteLine($"    Total I/O .....: {FormatBytes(io)}");
        Console.WriteLine($"    Throughput ....: {FormatBytes((long)(io / Math.Max(1.0, elapsed)))}/s");
        Console.WriteLine(line);
        Console.WriteLine($"    Msg Rate ......: {totalMsgs / Math.Max(1.0, elapsed):F0} msg/s");
        Console.WriteLine($"    Total Errors ..: {errs:N0}");

        var typeA = success.Where(r => DetermineClientType(r.Id, cfg.NumClients) == ClientType.ShortFast).ToList();
        var typeB = success.Where(r => DetermineClientType(r.Id, cfg.NumClients) == ClientType.HeavyTransfer).ToList();
        var typeC = success.Where(r => DetermineClientType(r.Id, cfg.NumClients) == ClientType.Bursty).ToList();

        Console.WriteLine(line);
        Console.WriteLine("  Client Type Breakdown:");
        Console.WriteLine($"    Type A (Short-Fast, {typeA.Count:N0} clients):");
        Console.WriteLine($"      Msgs: {typeA.Sum(r => r.MessagesSent):N0} | Sent: {FormatBytes(typeA.Sum(r => r.BytesSent))} | Recv: {FormatBytes(typeA.Sum(r => r.BytesReceived))}");
        Console.WriteLine($"    Type B (Heavy-Transfer, {typeB.Count:N0} clients):");
        Console.WriteLine($"      Msgs: {typeB.Sum(r => r.MessagesSent):N0} | Sent: {FormatBytes(typeB.Sum(r => r.BytesSent))} | Recv: {FormatBytes(typeB.Sum(r => r.BytesReceived))}");
        Console.WriteLine($"    Type C (Bursty, {typeC.Count:N0} clients):");
        Console.WriteLine($"      Msgs: {typeC.Sum(r => r.MessagesSent):N0} | Sent: {FormatBytes(typeC.Sum(r => r.BytesSent))} | Recv: {FormatBytes(typeC.Sum(r => r.BytesReceived))}");

        if (stats.PayloadSizes.Count > 0)
        {
            var ps = stats.PayloadSizes.ToList();
            Console.WriteLine(line);
            Console.WriteLine("  Payload Size Distribution:");
            Console.WriteLine($"    Samples .....: {ps.Count:N0}");
            Console.WriteLine($"    Min .........: {ps.Min():N0} B");
            Console.WriteLine($"    Max .........: {ps.Max():N0} B");
            Console.WriteLine($"    Avg .........: {ps.Average():F0} B");
        }

        if (stats.Intervals.Count > 0)
        {
            var iv = stats.Intervals.ToList();
            Console.WriteLine("  Send Interval Distribution:");
            Console.WriteLine($"    Samples .....: {iv.Count:N0}");
            Console.WriteLine($"    Min .........: {iv.Min():F1} ms");
            Console.WriteLine($"    Max .........: {iv.Max():F1} ms");
            Console.WriteLine($"    Avg .........: {iv.Average():F1} ms");
        }

        Console.WriteLine(sep);
        var passed = success.Count >= cfg.NumClients * 0.85;
        Console.WriteLine($"  Result: {(passed ? "[PASS]" : "[FAIL]")} {success.Count:N0}/{cfg.NumClients:N0} connected ({(double)success.Count / cfg.NumClients:P1})");
        Console.WriteLine(sep);

        if (failed.Count > 0)
        {
            var show = Math.Min(failed.Count, 15);
            Console.WriteLine($"\n  {failed.Count} clients failed (showing first {show}):");
            foreach (var f in failed.Take(show))
                Console.WriteLine($"    #{f.Id}: {f.ErrorMessage}");
        }
    }

    // ========================================================================
    // RESILIENCE TEST
    // ========================================================================

    static async Task RunResilienceTest(Args cfg)
    {
        var sep = new string('=', 80);
        var line = new string('-', 80);

        Console.WriteLine(sep);
        Console.WriteLine("  RESILIENCE TEST — Backend Failure Injection");
        Console.WriteLine(sep);
        Console.WriteLine($"  Clients ......: {cfg.NumClients:N0}");
        Console.WriteLine($"  Duration .....: {cfg.DurationSec} seconds");
        Console.WriteLine($"  Batch Size ...: {cfg.BatchSize} per wave");
        Console.WriteLine($"  Target .......: {cfg.Host}:{cfg.Port}");
        Console.WriteLine($"  Kill Server ..: port {cfg.KillServerPort} at ~15s mark");
        Console.WriteLine(line);
        Console.WriteLine($"  Payload size : fully random (64 B ~ 8,192 B)");
        Console.WriteLine($"  Send interval: fully random (5 ms ~ 500 ms)");
        Console.WriteLine(sep);
        Console.WriteLine();

        var stats = new TestStats();
        var sw = Stopwatch.StartNew();

        var killTask = Task.Run(async () =>
        {
            await Task.Delay(15_000);
            Console.WriteLine($"\n[{sw.Elapsed.TotalSeconds:F1}s] >>> KILLING backend server on port {cfg.KillServerPort} <<<");
            try
            {
                var psi = new ProcessStartInfo
                {
                    FileName = "/bin/bash",
                    Arguments = $"-c \"fuser -k {cfg.KillServerPort}/tcp 2>/dev/null\"",
                    RedirectStandardOutput = true,
                    UseShellExecute = false,
                };
                using var proc = Process.Start(psi);
                await proc.WaitForExitAsync();
                Console.WriteLine($"[{sw.Elapsed.TotalSeconds:F1}s] >>> Backend server on :{cfg.KillServerPort} killed <<<\n");
            }
            catch (Exception ex) { Console.WriteLine($"[!] Kill failed: {ex.Message}"); }
        });

        var totalBatches = (int)Math.Ceiling((double)cfg.NumClients / cfg.BatchSize);
        var tasks = new List<Task<ClientResult>>();

        for (int b = 0; b < totalBatches; b++)
        {
            int startId = b * cfg.BatchSize;
            int count = Math.Min(cfg.BatchSize, cfg.NumClients - startId);
            Console.WriteLine($"[{sw.Elapsed.TotalSeconds:F1}s] Wave {b + 1}/{totalBatches}: {count} clients (#{startId}..#{startId + count - 1})");

            for (int i = startId; i < startId + count; i++)
                tasks.Add(ResilienceClientWorker(i, cfg, stats));

            await Task.Delay(30);
        }

        Console.WriteLine($"\n[{sw.Elapsed.TotalSeconds:F1}s] All {cfg.NumClients:N0} clients launched. Waiting for completion...");
        var allResults = await Task.WhenAll(tasks);
        await killTask;
        sw.Stop();
        PrintResilienceReport(allResults, cfg, stats, sw.Elapsed.TotalSeconds, sep, line);
    }

    static async Task<ClientResult> ResilienceClientWorker(int id, Args cfg, TestStats stats)
    {
        var result = new ClientResult { Id = id };
        try
        {
            using var client = new TcpSocketClient(cfg.Host, cfg.Port);
            var connSw = Stopwatch.StartNew();
            await Task.WhenAny(client.ConnectAsync(), Task.Delay(8000));
            connSw.Stop();

            if (!client.IsConnected) { result.ErrorMessage = "Connection timeout after 8s"; return result; }
            result.ConnectSuccess = true;
            result.ConnectTimeMs = connSw.Elapsed.TotalMilliseconds;
            stats.RecordConnect(connSw.Elapsed.TotalMilliseconds);

            var rng = new Random(id * 37 + 13);
            long recvBytes = 0, sentBytes = 0;
            int msgCount = 0, errCount = 0;

            client.DataReceived += text => Interlocked.Add(ref recvBytes, Encoding.UTF8.GetByteCount(text));
            var endTime = DateTime.UtcNow.AddSeconds(cfg.DurationSec);

            while (DateTime.UtcNow < endTime && client.IsConnected)
            {
                try
                {
                    int pSize = 64 + rng.Next(0, 8128);
                    var payload = GenPayload(pSize, id, msgCount);
                    int interval = 5 + rng.Next(0, 495);

                    await client.SendAsync(payload);
                    Interlocked.Add(ref sentBytes, payload.Length);
                    msgCount++;
                    stats.RecordPayloadSize(pSize);
                    stats.RecordInterval(interval);
                    await Task.Delay(interval);
                } catch { errCount++; }
            }

            result.MessagesSent = msgCount;
            result.BytesSent = sentBytes;
            result.BytesReceived = recvBytes;
            result.Errors = errCount;
        }
        catch (Exception ex) { result.ErrorMessage = ex.Message; }
        return result;
    }

    static void PrintResilienceReport(ClientResult[] results, Args cfg, TestStats stats, double elapsed, string sep, string line)
    {
        var success = results.Where(r => r.ConnectSuccess).ToList();
        var failed = results.Where(r => !r.ConnectSuccess).ToList();
        var totalMsgs = success.Sum(r => r.MessagesSent);
        var sent = success.Sum(r => r.BytesSent);
        var recv = success.Sum(r => r.BytesReceived);
        var errs = success.Sum(r => r.Errors);
        var io = sent + recv;

        Console.WriteLine();
        Console.WriteLine(sep);
        Console.WriteLine("  RESILIENCE TEST REPORT");
        Console.WriteLine(sep);
        Console.WriteLine($"  Duration ..........: {elapsed:F2} s");
        Console.WriteLine(line);
        Console.WriteLine($"  Total Clients .....: {cfg.NumClients:N0}");
        Console.WriteLine($"  Successful Connect : {success.Count:N0}");
        Console.WriteLine($"  Failed Connect ....: {failed.Count:N0} ({(double)failed.Count / cfg.NumClients:P2})");
        Console.WriteLine(line);

        if (stats.ConnectTimes.Count > 0)
        {
            var times = stats.ConnectTimes.ToList();
            Console.WriteLine("  Connection Latency:");
            Console.WriteLine($"    Avg .......: {times.Average():F2} ms");
            Console.WriteLine($"    P50 .......: {Percentile(times, 50):F2} ms");
            Console.WriteLine($"    P95 .......: {Percentile(times, 95):F2} ms");
            Console.WriteLine($"    P99 .......: {Percentile(times, 99):F2} ms");
            Console.WriteLine(line);
        }

        Console.WriteLine("  Data Transfer:");
        Console.WriteLine($"    Total Msgs ....: {totalMsgs:N0}");
        Console.WriteLine($"    Bytes Sent ....: {FormatBytes(sent)}");
        Console.WriteLine($"    Bytes Recv ....: {FormatBytes(recv)}");
        Console.WriteLine($"    Total I/O .....: {FormatBytes(io)}");
        Console.WriteLine($"    Throughput ....: {FormatBytes((long)(io / Math.Max(1.0, elapsed)))}/s");
        Console.WriteLine($"    Msg Rate ......: {totalMsgs / Math.Max(1.0, elapsed):F0} msg/s");
        Console.WriteLine(line);
        Console.WriteLine($"    Total Errors ..: {errs:N0}");

        if (stats.PayloadSizes.Count > 0)
        {
            var ps = stats.PayloadSizes.ToList();
            Console.WriteLine("  Payload Size Distribution:");
            Console.WriteLine($"    Samples .....: {ps.Count:N0}");
            Console.WriteLine($"    Min .........: {ps.Min():N0} B");
            Console.WriteLine($"    Max .........: {ps.Max():N0} B");
            Console.WriteLine($"    Avg .........: {ps.Average():F0} B");
        }

        if (stats.Intervals.Count > 0)
        {
            var iv = stats.Intervals.ToList();
            Console.WriteLine("  Send Interval Distribution:");
            Console.WriteLine($"    Samples .....: {iv.Count:N0}");
            Console.WriteLine($"    Min .........: {iv.Min():F1} ms");
            Console.WriteLine($"    Max .........: {iv.Max():F1} ms");
            Console.WriteLine($"    Avg .........: {iv.Average():F1} ms");
        }

        Console.WriteLine(sep);
        var passed = success.Count >= cfg.NumClients * 0.80;
        double ratio = (double)success.Count / cfg.NumClients;
        Console.WriteLine($"  Result: {(passed ? "[PASS]" : "[FAIL]")} {success.Count:N0}/{cfg.NumClients:N0} connected ({ratio:P1})");
        if (!passed) Console.WriteLine("  [!] WARNING: Connection ratio below 80%.");
        Console.WriteLine(sep);

        if (failed.Count > 0)
        {
            var show = Math.Min(failed.Count, 15);
            Console.WriteLine($"\n  {failed.Count} clients failed (showing first {show}):");
            foreach (var f in failed.Take(show))
                Console.WriteLine($"    #{f.Id}: {f.ErrorMessage}");
        }
    }
}