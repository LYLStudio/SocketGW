using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text;
using SocketClientLib;

Console.Title = "Socket Resilience Test - 100K Clients + Backend Failure Simulation";

await RunResilienceTestAsync();

static async Task RunResilienceTestAsync()
{
    // ===== 配置 =====
    var numClients = int.TryParse(Environment.GetEnvironmentVariable("NUM_CLIENTS"), out var nc) ? nc : 100_000;
    var durationSec = int.TryParse(Environment.GetEnvironmentVariable("DURATION_SEC"), out var ds) ? ds : 45;
    var host = Environment.GetEnvironmentVariable("SERVER_HOST") ?? "127.0.0.1";
    var port = int.TryParse(Environment.GetEnvironmentVariable("SERVER_PORT"), out var p) ? p : 8080;
    var batchSize = int.TryParse(Environment.GetEnvironmentVariable("BATCH_SIZE"), out var bs) ? bs : 1000;
    var killServerPort = int.TryParse(Environment.GetEnvironmentVariable("KILL_SERVER_PORT"), out var kp) ? kp : 5002;

    var sep = new string('=', 80);
    var line = new string('-', 80);

    Console.WriteLine(sep);
    Console.WriteLine("  SOCKET RESILIENCE TEST — 100K Clients + Backend Failure Simulation");
    Console.WriteLine(sep);
    Console.WriteLine($"  Clients ......: {numClients:N0}");
    Console.WriteLine($"  Duration .....: {durationSec} seconds");
    Console.WriteLine($"  Batch Size ...: {batchSize} per wave");
    Console.WriteLine($"  Target .......: {host}:{port}");
    Console.WriteLine($"  Kill Server ..: port {killServerPort} at ~15s mark");
    Console.WriteLine(line);
    Console.WriteLine($"  Payload size : fully random (64 B ~ 8,192 B)");
    Console.WriteLine($"  Send interval: fully random (5 ms ~ 500 ms)");
    Console.WriteLine(sep);
    Console.WriteLine();

    // ===== Thread-safe Stats =====
    var stats = new ResilienceStats();
    var sw = Stopwatch.StartNew();

    // ===== Kill one backend server at 15s mark =====
    var killTask = Task.Run(async () =>
    {
        await Task.Delay(15_000);
        Console.WriteLine($"\n[{sw.Elapsed.TotalSeconds:F1}s] >>> KILLING backend server on port {killServerPort} <<<");

        // Use fuser to find and kill the process
        var startInfo = new System.Diagnostics.ProcessStartInfo
        {
            FileName = "/bin/bash",
            Arguments = $"-c \"fuser -k {killServerPort}/tcp 2>/dev/null\"",
            RedirectStandardOutput = true,
            UseShellExecute = false,
        };
        try
        {
            using var proc = System.Diagnostics.Process.Start(startInfo);
            await proc.WaitForExitAsync();
            Console.WriteLine($"[{sw.Elapsed.TotalSeconds:F1}s] >>> Backend server on :{killServerPort} killed <<<\n");
            stats.RecordFailureInjection();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[!] Kill failed: {ex.Message}");
        }
    });

    // ===== Launch clients in batches =====
    var totalBatches = (int)Math.Ceiling((double)numClients / batchSize);
    var clientTasks = new List<Task<ResilienceClientResult>>();

    for (int b = 0; b < totalBatches; b++)
    {
        int startId = b * batchSize;
        int count = Math.Min(batchSize, numClients - startId);

        Console.WriteLine($"[{sw.Elapsed.TotalSeconds:F1}s] Wave {b +1}/{totalBatches}: {count} clients (#{startId}..#{startId + count - 1})");

        for (int i = startId; i < startId + count; i++)
        {
            clientTasks.Add(ClientWorkerAsync(i, host, port, durationSec, stats));
        }

        await Task.Delay(30); // stagger between waves
    }

    Console.WriteLine($"\n[{sw.Elapsed.TotalSeconds:F1}s] All {numClients:N0} clients launched. Waiting for completion...");

    // ===== Wait for all =====
    var results = await Task.WhenAll(clientTasks);
    await killTask;

    sw.Stop();

    // ===== Aggregate =====
    int successCount = stats.ConnectSuccess.Count;
    int failCount = numClients - successCount;
    long totalMsgs = 0, totalSent = 0, totalRecv = 0, totalErrors = 0;
    int disconnectsDuringRun = 0;

    foreach (var r in results)
    {
        if (!r.ConnectSuccess) continue;
        totalMsgs += r.MessagesSent;
        totalSent += r.BytesSent;
        totalRecv += r.BytesReceived;
        totalErrors += r.Errors;
        if (r.DisconnectedMidTest) disconnectsDuringRun++;
    }

    var connectTimes = stats.ConnectTimes.ToList();
    double avgConn = connectTimes.Average();
    double p50 = Percentile(connectTimes, 50);
    double p95 = Percentile(connectTimes, 95);
    double p99 = Percentile(connectTimes, 99);

    // ===== Report =====
    Console.WriteLine();
    Console.WriteLine(sep);
    Console.WriteLine("  RESILIENCE TEST REPORT");
    Console.WriteLine(sep);
    Console.WriteLine($"  Duration ..........: {sw.Elapsed.TotalSeconds:F2} s");
    Console.WriteLine(line);
    Console.WriteLine($"  Total Clients .....: {numClients:N0}");
    Console.WriteLine($"  Successful Connect : {successCount:N0}");
    Console.WriteLine($"  Failed Connect ....: {failCount:N0} ({(double)failCount / numClients:P2})");
    Console.WriteLine(line);
    Console.WriteLine("  Connection Latency:");
    Console.WriteLine($"    Avg .......: {avgConn:F2} ms");
    Console.WriteLine($"    P50 .......: {p50:F2} ms");
    Console.WriteLine($"    P95 .......: {p95:F2} ms");
    Console.WriteLine($"    P99 .......: {p99:F2} ms");
    Console.WriteLine(line);

    var totalIO = totalSent + totalRecv;
    var elapsedSec = Math.Max(1.0, sw.Elapsed.TotalSeconds);
    Console.WriteLine("  Data Transfer:");
    Console.WriteLine($"    Total Msgs ....: {totalMsgs:N0}");
    Console.WriteLine($"    Bytes Sent ....: {FormatBytes(totalSent)}");
    Console.WriteLine($"    Bytes Recv ....: {FormatBytes(totalRecv)}");
    Console.WriteLine($"    Total I/O .....: {FormatBytes(totalIO)}");
    Console.WriteLine($"    Throughput ....: {FormatBytes((long)(totalIO / elapsedSec))}/s");
    Console.WriteLine($"    Msg Rate ......: {totalMsgs / elapsedSec:F0} msg/s");
    Console.WriteLine(line);
    Console.WriteLine($"    Total Errors ..: {totalErrors:N0}");
    Console.WriteLine($"  Mid-test Disconnects (fault indicator): {disconnectsDuringRun:N0}");
    Console.WriteLine(line);

    // Payload / interval stats
    if (stats.PayloadSizes.Count > 0)
    {
        var ps = stats.PayloadSizes.ToList();
        Console.WriteLine("  Payload Size Distribution:");
        Console.WriteLine($"    Samples .....: {ps.Count:N0}");
        Console.WriteLine($"    Min .........: {ps.Min()} B");
        Console.WriteLine($"    Max .........: {ps.Max()} B");
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

    // Pass criteria: >= 80% connected is acceptable under fault injection
    double connectRatio = (double)successCount / numClients;
    bool passed = connectRatio >= 0.80;
    Console.WriteLine($"  Result: {(passed ? "[PASS]" : "[FAIL]")} {successCount:N0}/{numClients:N0} connected ({connectRatio:P1})");

    if (!passed)
        Console.WriteLine("  [!] WARNING: Connection ratio below 80% — Gateway may not be handling backend failure properly.");

    Console.WriteLine(sep);

    // Show failed clients
    var failed = results.Where(r => !r.ConnectSuccess).ToList();
    if (failed.Count > 0 && failed.Count <= 20)
    {
        Console.WriteLine("\n  Failed Clients:");
        foreach (var f in failed)
            Console.WriteLine($"    #{f.Id}: {f.ErrorMessage}");
    }
    else if (failed.Count > 20)
    {
        Console.WriteLine($"\n  {failed.Count:N0} clients failed (first 15):");
        foreach (var f in failed.Take(15))
            Console.WriteLine($"    #{f.Id}: {f.ErrorMessage}");
    }
}

// ===== Per-Client Worker =====
static async Task<ResilienceClientResult> ClientWorkerAsync(
    int id, string host, int port, int durationSec, ResilienceStats stats)
{
    var result = new ResilienceClientResult { Id = id };

    try
    {
        using var client = new TcpSocketClient(host, port);

        var connSw = Stopwatch.StartNew();
        await Task.WhenAny(
            client.ConnectAsync(),
            Task.Delay(8000) // 8s connect timeout
        );
        connSw.Stop();

        if (!client.IsConnected)
        {
            result.ConnectSuccess = false;
            result.ErrorMessage = "Connection timeout after 8s";
            return result;
        }

        result.ConnectSuccess = true;
        result.ConnectTimeMs = connSw.Elapsed.TotalMilliseconds;
        stats.RecordConnect(connSw.Elapsed.TotalMilliseconds);

        var rng = new Random(id * 37 + 13); // unique per client
        long recvBytes = 0;
        long sendBytes = 0;
        int msgCount = 0;
        int errCount = 0;

        client.DataReceived += (text) =>
        {
            Interlocked.Add(ref recvBytes, Encoding.UTF8.GetByteCount(text));
        };

        var endTime = DateTime.UtcNow.AddSeconds(durationSec);

        while (DateTime.UtcNow < endTime && client.IsConnected)
        {
            try
            {
                // Fully random payload size: 64 ~ 8192 bytes
                int payloadSize = 64 + rng.Next(0, 8128);
                var payload = MakePayload(payloadSize, id, msgCount);

                // Fully random interval: 5 ~ 500 ms
                int interval = 5 + rng.Next(0, 495);

                await client.SendAsync(payload);
                Interlocked.Add(ref sendBytes, payload.Length);
                msgCount++;

                stats.RecordPayloadSize(payloadSize);
                stats.RecordInterval(interval);

                await Task.Delay(interval);
            }
            catch
            {
                errCount++;
            }
        }

        result.MessagesSent = msgCount;
        result.BytesSent = sendBytes;
        result.BytesReceived = recvBytes;
        result.Errors = errCount;
        result.DisconnectedMidTest = !client.IsConnected;
    }
    catch (Exception ex)
    {
        result.ConnectSuccess = false;
        result.ErrorMessage = ex.Message;
    }

    return result;
}

static byte[] MakePayload(int size, int id, int seq)
{
    var header = $"RES:{id}:{seq}:S={size}";
    var hBytes = Encoding.ASCII.GetBytes(header);
    var buf = new byte[size];
    Buffer.BlockCopy(hBytes, 0, buf, 0, Math.Min(hBytes.Length, size));
    for (int i = Math.Min(hBytes.Length, size); i < size; i++)
        buf[i] = (byte)((id + seq + i) % 256);
    return buf;
}

static double Percentile(List<double> vals, double pct)
{
    if (vals.Count == 0) return 0;
    vals.Sort();
    int idx = Math.Max(0, (int)Math.Ceiling(pct / 100.0 * vals.Count) - 1);
    return vals[idx];
}

static string FormatBytes(long b) => b switch
{
    < 0 => "N/A",
    < 1024 => $"{b} B",
    < 1024 * 1024 => $"{b / 1024.0:F2} KB",
    < 1024 * 1024 * 1024 => $"{b / (1024.0 * 1024):F2} MB",
    _ => $"{b / (1024.0 * 1024 * 1024):F2} GB"
};

// ===== Data classes =====
sealed class ResilienceStats
{
    public ConcurrentBag<int> ConnectSuccess = new();
    public ConcurrentBag<double> ConnectTimes = new();
    public ConcurrentBag<int> PayloadSizes = new();
    public ConcurrentBag<double> Intervals = new();
    public bool FailureInjected { get; set; }

    public void RecordConnect(double ms) { ConnectSuccess.Add(1); ConnectTimes.Add(ms); }
    public void RecordPayloadSize(int s) => PayloadSizes.Add(s);
    public void RecordInterval(double ms) => Intervals.Add(ms);
    public void RecordFailureInjection() => FailureInjected = true;
}

sealed class ResilienceClientResult
{
    public int Id { get; set; }
    public bool ConnectSuccess { get; set; }
    public string? ErrorMessage { get; set; }
    public double ConnectTimeMs { get; set; }
    public int MessagesSent { get; set; }
    public long BytesSent { get; set; }
    public long BytesReceived { get; set; }
    public int Errors { get; set; }
    public bool DisconnectedMidTest { get; set; }
}