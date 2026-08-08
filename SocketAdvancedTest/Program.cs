using System.Collections.Concurrent;
using System.Text;
using SocketClientLib;

Console.Title = "Socket Advanced Stress Test";

await RunAdvancedTestAsync();

static async Task RunAdvancedTestAsync()
{
    // ===== 測試配置 =====
    var numClients = int.TryParse(Environment.GetEnvironmentVariable("NUM_CLIENTS"), out var nc) ? nc : 10_000;
    var durationSec = int.TryParse(Environment.GetEnvironmentVariable("DURATION_SEC"), out var ds) ? ds : 30;
    var host = Environment.GetEnvironmentVariable("SERVER_HOST") ?? "127.0.0.1";
    var port = int.TryParse(Environment.GetEnvironmentVariable("SERVER_PORT"), out var p) ? p : 8080;
    var batchSize = int.TryParse(Environment.GetEnvironmentVariable("BATCH_SIZE"), out var bs) ? bs : 500;

    var separator = new string('=', 72);
    var thinLine = new string('-', 72);

    Console.WriteLine(separator);
    Console.WriteLine("  Socket Advanced Stress Test (Variable Payload + Random Intervals)");
    Console.WriteLine(separator);
    Console.WriteLine($"  Total Clients .: {numClients:N0}");
    Console.WriteLine($"  Duration .....: {durationSec} seconds");
    Console.WriteLine($"  Batch Size ...: {batchSize} (staggered connect)");
    Console.WriteLine($"  Target .......: {host}:{port}");
    Console.WriteLine(thinLine);
    Console.WriteLine($"  Type A (60%): short-fast clients — small payload, fast send");
    Console.WriteLine($"  Type B (25%): heavy-transfer clients — large payload, medium send");
    Console.WriteLine($"  Type C (15%): bursty clients — random bursts with idle gaps");
    Console.WriteLine(separator);
    Console.WriteLine();

    // ===== Thread-safe statistics =====
    var stats = new AdvancedTestStats();
    var globalStopwatch = System.Diagnostics.Stopwatch.StartNew();

    // ===== Staggered batch launching =====
    var totalBatches = (int)Math.Ceiling((double)numClients / batchSize);
    var clientTasks = new List<Task<ClientResult>>();

    for (int b = 0; b < totalBatches; b++)
    {
        int startId = b * batchSize;
        int countInBatch = Math.Min(batchSize, numClients - startId);

        Console.WriteLine($"[{globalStopwatch.Elapsed.TotalSeconds:F1}s] Batch {b +1}/{totalBatches}: launching {countInBatch} clients (#{startId}..#{startId + countInBatch - 1})...");

        for (int i = startId; i < startId + countInBatch; i++)
        {
            var clientType = DetermineClientType(i, numClients);
            clientTasks.Add(ClientWorkerAsync(i, host, port, durationSec, clientType, stats));
        }

        // Stagger batches: wait a short time before next batch
        if (b < totalBatches - 1)
        {
            await Task.Delay(50);
        }
    }

    Console.WriteLine($"[{globalStopwatch.Elapsed.TotalSeconds:F1}s] All clients launched. Waiting for completion...");
    Console.WriteLine();

    // ===== Wait for all to finish =====
    var results = await Task.WhenAll(clientTasks);
    globalStopwatch.Stop();

    // ===== Aggregate stats =====
    long totalSent = 0, totalRecv = 0, totalMsgs = 0, totalErrors = 0;
    int successCount = 0;

    var typeAStats = new List<(int msgs, long sent, long recv, int errs)>();
    var typeBStats = new List<(int msgs, long sent, long recv, int errs)>();
    var typeCStats = new List<(int msgs, long sent, long recv, int errs)>();

    foreach (var r in results)
    {
        if (!r.ConnectSuccess) continue;
        successCount++;

        totalMsgs += r.MessagesSent;
        totalSent += r.BytesSent;
        totalRecv += r.BytesReceived;
        totalErrors += r.Errors;

        switch (r.ClientType)
        {
            case ClientType.ShortFast:   typeAStats.Add((r.MessagesSent, r.BytesSent, r.BytesReceived, r.Errors)); break;
            case ClientType.HeavyTransfer: typeBStats.Add((r.MessagesSent, r.BytesSent, r.BytesReceived, r.Errors)); break;
            case ClientType.Bursty:     typeCStats.Add((r.MessagesSent, r.BytesSent, r.BytesReceived, r.Errors)); break;
        }
    }

    // ===== Report =====
    Console.WriteLine(separator);
    Console.WriteLine("  Advanced Stress Test Report");
    Console.WriteLine(separator);
    Console.WriteLine($"  Duration ..........: {globalStopwatch.Elapsed.TotalSeconds:F2} s");
    Console.WriteLine(thinLine);

    var failCount = numClients - stats.ConnectSuccess.Count;
    Console.WriteLine($"  Total Clients .....: {numClients:N0}");
    Console.WriteLine($"  Successful Connect : {stats.ConnectSuccess.Count:N0}");
    Console.WriteLine($"  Failed Connect ....: {failCount:N0}");
    Console.WriteLine(thinLine);

    // Connect time stats
    if (stats.ConnectSuccess.Count > 0)
    {
        var connectList = stats.ConnectTimes.ToList();
        var avgConnect = connectList.Average();
        var p50 = Percentile(connectList, 50);
        var p95 = Percentile(connectList, 95);
        var p99 = Percentile(connectList, 99);

        Console.WriteLine("  Connection Latency:");
        Console.WriteLine($"    Avg .......: {avgConnect:F2} ms");
        Console.WriteLine($"    P50 .......: {p50:F2} ms");
        Console.WriteLine($"    P95 .......: {p95:F2} ms");
        Console.WriteLine($"    P99 .......: {p99:F2} ms");
        Console.WriteLine(thinLine);
    }

    // Data transfer
    var totalIO = totalSent + totalRecv;
    var throughput = totalIO / Math.Max(1.0, globalStopwatch.Elapsed.TotalSeconds);
    var msgRate = totalMsgs / Math.Max(1.0, globalStopwatch.Elapsed.TotalSeconds);

    Console.WriteLine("  Data Transfer:");
    Console.WriteLine($"    Total Msgs ....: {totalMsgs:N0}");
    Console.WriteLine($"    Bytes Sent ....: {FormatBytes(totalSent)}");
    Console.WriteLine($"    Bytes Received : {FormatBytes(totalRecv)}");
    Console.WriteLine($"    Total I/O .....: {FormatBytes(totalIO)}");
    Console.WriteLine($"    Throughput ....: {FormatBytes((long)throughput)}/s");
    Console.WriteLine(thinLine);
    Console.WriteLine($"    Msg Rate ......: {msgRate:F0} msg/s");
    Console.WriteLine($"    Total Errors ..: {totalErrors:N0}");
    Console.WriteLine(thinLine);

    // Per-type breakdown
    Console.WriteLine("  Client Type Breakdown:");
    Console.WriteLine($"    Type A (Short-Fast, {(typeAStats.Count):N0} clients):");
    Console.WriteLine($"      Msgs: {typeAStats.Sum(t => t.msgs):N0} | Sent: {FormatBytes(typeAStats.Sum(t => t.sent))} | Recv: {FormatBytes(typeAStats.Sum(t => t.recv))}");
    Console.WriteLine($"    Type B (Heavy-Transfer, {(typeBStats.Count):N0} clients):");
    Console.WriteLine($"      Msgs: {typeBStats.Sum(t => t.msgs):N0} | Sent: {FormatBytes(typeBStats.Sum(t => t.sent))} | Recv: {FormatBytes(typeBStats.Sum(t => t.recv))}");
    Console.WriteLine($"    Type C (Bursty, {(typeCStats.Count):N0} clients):");
    Console.WriteLine($"      Msgs: {typeCStats.Sum(t => t.msgs):N0} | Sent: {FormatBytes(typeCStats.Sum(t => t.sent))} | Recv: {FormatBytes(typeCStats.Sum(t => t.recv))}");
    Console.WriteLine(thinLine);

    // Payload size distribution
    Console.WriteLine("  Payload Size Distribution (across all messages):");
    Console.WriteLine($"    Total unique payloads sent: {stats.TotalPayloadSizes}");
    if (stats.PayloadSizeSamples.Count > 0)
    {
        var samples = stats.PayloadSizeSamples.ToList();
        Console.WriteLine($"    Min size ....: {samples.Min():N0} B");
        Console.WriteLine($"    Max size ....: {samples.Max():N0} B");
        Console.WriteLine($"    Avg size ....: {samples.Average():F0} B");
    }

    // Interval distribution
    if (stats.SendIntervalSamples.Count > 0)
    {
        var intervals = stats.SendIntervalSamples.ToList();
        Console.WriteLine("  Send Interval Distribution:");
        Console.WriteLine($"    Min interval : {intervals.Min():F1} ms");
        Console.WriteLine($"    Max interval : {intervals.Max():F1} ms");
        Console.WriteLine($"    Avg interval : {intervals.Average():F1} ms");
    }

    Console.WriteLine(separator);

    var passed = stats.ConnectSuccess.Count >= numClients * 0.85; // 85% threshold for large scale
    Console.WriteLine($"  Result: {(passed ? "[PASS]" : "[FAIL]")} {stats.ConnectSuccess.Count:N0}/{numClients:N0} clients connected ({(double)stats.ConnectSuccess.Count / numClients:P1})");
    Console.WriteLine(separator);

    // ===== Failed client details =====
    var failedResults = results.Where(r => !r.ConnectSuccess).ToList();
    if (failedResults.Count > 0 && failedResults.Count <= 20)
    {
        Console.WriteLine("\n  Failed Clients:");
        foreach (var f in failedResults)
            Console.WriteLine($"    Client #{f.Id} ({f.TypeLabel}): {f.ErrorMessage}");
    }
    else if (failedResults.Count > 20)
    {
        Console.WriteLine($"\n  {failedResults.Count:N0} clients failed (showing first 15):");
        foreach (var f in failedResults.Take(15))
            Console.WriteLine($"    Client #{f.Id} ({f.TypeLabel}): {f.ErrorMessage}");
    }
}

// ===== Client Type Determination =====
static ClientType DetermineClientType(int clientId, int totalClients)
{
    // 60% Type A, 25% Type B, 15% Type C (deterministic per client id)
    double ratio = (double)(clientId % 100) / 100.0;
    return ratio < 0.60 ? ClientType.ShortFast :
           ratio < 0.85 ? ClientType.HeavyTransfer :
                          ClientType.Bursty;
}

// ===== Per-Client Worker =====
static async Task<ClientResult> ClientWorkerAsync(
    int clientId, string host, int port, int durationSec,
    ClientType type, AdvancedTestStats stats)
{
    var result = new ClientResult { Id = clientId, ClientType = type };

    try
    {
        using var client = new TcpSocketClient(host, port);

        var connectSw = System.Diagnostics.Stopwatch.StartNew();
        await Task.WhenAny(
            client.ConnectAsync(),
            Task.Delay(5000) // 5s connect timeout
        );
        connectSw.Stop();

        if (!client.IsConnected)
        {
            result.ConnectSuccess = false;
            result.ErrorMessage = "Connection timed out after 5s";
            result.TypeLabel = type.ToString();
            return result;
        }

        result.ConnectSuccess = true;
        result.ConnectTimeMs = connectSw.Elapsed.TotalMilliseconds;
        stats.RecordConnect(connectSw.Elapsed.TotalMilliseconds);

        var rng = new Random(clientId * 31 + 7); // deterministic per client
        long receiveBytes = 0;

        client.DataReceived += (text) =>
        {
            Interlocked.Add(ref receiveBytes, Encoding.UTF8.GetByteCount(text));
        };

        var sendCount = 0;
        var errorCount = 0;
        long bytesSentAccum = 0;

        var endTime = DateTime.UtcNow.AddSeconds(durationSec);

        while (DateTime.UtcNow < endTime && client.IsConnected)
        {
            try
            {
                byte[] payload = Array.Empty<byte>();
                int delayMs;

                switch (type)
                {
                    case ClientType.ShortFast:
                        // Small payload (64~256 bytes), fast send (10~30ms interval)
                        var sizeA = 64 + rng.Next(0, 192);
                        payload = GeneratePayload(sizeA, clientId, sendCount);
                        delayMs = 10 + rng.Next(0, 20);
                        break;

                    case ClientType.HeavyTransfer:
                        // Large payload (512~4096 bytes), medium interval (50~150ms)
                        var sizeB = 512 + rng.Next(0, 3584);
                        payload = GeneratePayload(sizeB, clientId, sendCount);
                        delayMs = 50 + rng.Next(0, 100);
                        break;

                    case ClientType.Bursty:
                        // Random bursts: either send 5~15 messages rapidly then idle, or single large
                        if (rng.Next(0, 3) == 0)
                        {
                            // Burst mode
                            for (int burst = 0; burst < rng.Next(5, 16); burst++)
                            {
                                var sizeC = 64 + rng.Next(0, 2048);
                                payload = GeneratePayload(sizeC, clientId, sendCount);
                                await client.SendAsync(payload);
                                Interlocked.Add(ref bytesSentAccum, payload.Length);
                                stats.RecordPayloadSize(payload.Length);
                                sendCount++;
                                await Task.Delay(rng.Next(1, 5)); // very fast within burst
                            }
                            delayMs = 200 + rng.Next(0, 500); // long idle after burst
                        }
                        else
                        {
                            var sizeC = 128 + rng.Next(0, 1024);
                            payload = GeneratePayload(sizeC, clientId, sendCount);
                            delayMs = 30 + rng.Next(0, 200);
                        }
                        break;

                    default:
                        var sizeD = 128;
                        payload = GeneratePayload(sizeD, clientId, sendCount);
                        delayMs = 50;
                        break;
                }

                if (type != ClientType.Bursty || rng.Next(0, 3) != 0)
                {
                    await client.SendAsync(payload);
                    Interlocked.Add(ref bytesSentAccum, payload.Length);
                    stats.RecordPayloadSize(payload.Length);
                    sendCount++;
                }

                stats.RecordSendInterval(delayMs);
                await Task.Delay(delayMs);
            }
            catch
            {
                errorCount++;
            }
        }

        result.MessagesSent = sendCount;
        result.BytesSent = bytesSentAccum;
        result.BytesReceived = receiveBytes;
        result.Errors = errorCount;
        result.TypeLabel = type.ToString();
    }
    catch (Exception ex)
    {
        result.ConnectSuccess = false;
        result.ErrorMessage = ex.Message;
        result.TypeLabel = type.ToString();
    }

    return result;
}

static byte[] GeneratePayload(int size, int clientId, int msgSeq)
{
    // Deterministic but variable-content payload
    var header = $"MSG:{clientId}:{msgSeq}:LEN={size}";
    var headerBytes = Encoding.ASCII.GetBytes(header);

    var payload = new byte[size];
    Buffer.BlockCopy(headerBytes, 0, payload, 0, Math.Min(headerBytes.Length, size));

    // Fill rest with repeating pattern based on client id & sequence
    var fillByte = (byte)((clientId + msgSeq) % 256);
    for (int i = Math.Min(headerBytes.Length, size); i < size; i++)
        payload[i] = (byte)(fillByte + i % 10);

    return payload;
}

static double Percentile(List<double> values, double percentile)
{
    if (values.Count == 0) return 0;
    var list = values;
    list.Sort();
    int index = (int)Math.Ceiling(percentile / 100.0 * list.Count) - 1;
    index = Math.Max(0, Math.Min(index, list.Count - 1));
    return list[index];
}

static string FormatBytes(long bytes) => bytes switch
{
    < 0 => "N/A",
    < 1024 => $"{bytes} B",
    < 1024 * 1024 => $"{bytes / 1024.0:F2} KB",
    < 1024 * 1024 * 1024 => $"{bytes / (1024.0 * 1024):F2} MB",
    _ => $"{bytes / (1024.0 * 1024 * 1024):F2} GB"
};

// ===== Enums & Data Classes =====
enum ClientType { ShortFast, HeavyTransfer, Bursty }

sealed class AdvancedTestStats
{
    public ConcurrentBag<int> ConnectSuccess = new();
    public ConcurrentBag<double> ConnectTimes = new();
    public ConcurrentBag<int> PayloadSizeSamples = new();
    public ConcurrentBag<double> SendIntervalSamples = new();
    public long TotalPayloadSizes => PayloadSizeSamples.Count;

    public void RecordConnect(double ms)
    {
        ConnectSuccess.Add(1);
        ConnectTimes.Add(ms);
    }

    public void RecordPayloadSize(int size) => PayloadSizeSamples.Add(size);
    public void RecordSendInterval(double ms) => SendIntervalSamples.Add(ms);
}

sealed class ClientResult
{
    public int Id { get; set; }
    public bool ConnectSuccess { get; set; }
    public string? ErrorMessage { get; set; }
    public double ConnectTimeMs { get; set; }
    public ClientType ClientType { get; set; }
    public string TypeLabel = "";
    public int MessagesSent { get; set; }
    public long BytesSent { get; set; }
    public long BytesReceived { get; set; }
    public int Errors { get; set; }
}