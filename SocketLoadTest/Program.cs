using System.Collections.Concurrent;
using System.Text;
using SocketClientLib;

Console.Title = "Socket Load Test";

await RunLoadTestAsync();

static async Task RunLoadTestAsync()
{
    // ===== 測試配置 =====
    var numClients = int.TryParse(Environment.GetEnvironmentVariable("NUM_CLIENTS"), out var nc) ? nc : 100;
    var durationSec = int.TryParse(Environment.GetEnvironmentVariable("DURATION_SEC"), out var ds) ? ds : 10;
    var host = Environment.GetEnvironmentVariable("SERVER_HOST") ?? "127.0.0.1";
    var port = int.TryParse(Environment.GetEnvironmentVariable("SERVER_PORT"), out var p) ? p : 5000;

    var separator = new string('=', 68);
    var thinLine = new string('-', 68);

    Console.WriteLine(separator);
    Console.WriteLine("  Socket Load Test");
    Console.WriteLine(separator);
    Console.WriteLine($"  Clients .....: {numClients}");
    Console.WriteLine($"  Duration ....: {durationSec} seconds");
    Console.WriteLine($"  Target ......: {host}:{port}");
    Console.WriteLine(separator);
    Console.WriteLine();

    // ===== 統計收集器 (thread-safe) =====
    var connectedTimes = new ConcurrentBag<double>();
    var clientTasks = new List<Task<ClientResult>>();
    var stopwatch = System.Diagnostics.Stopwatch.StartNew();

    Console.WriteLine($"[{stopwatch.Elapsed.TotalSeconds:F2}s] Starting {numClients} clients...");

    // ===== 啟動所有 Client =====
    for (int i = 0; i < numClients; i++)
    {
        clientTasks.Add(ClientWorkerAsync(i, host, port, durationSec, connectedTimes));
    }

    // ===== 等待全部完成 =====
    var results = await Task.WhenAll(clientTasks);

    stopwatch.Stop();

    // ===== 統計結果 =====
    var successResults = results.Where(r => r.ConnectSuccess).ToList();
    var failResults = results.Where(r => !r.ConnectSuccess).ToList();

    // 輸出報告
    Console.WriteLine();
    Console.WriteLine(separator);
    Console.WriteLine("  Load Test Report");
    Console.WriteLine(separator);
    Console.WriteLine($"  Duration ..........: {stopwatch.Elapsed.TotalSeconds:F2} s");
    Console.WriteLine(thinLine);
    Console.WriteLine($"  Total Clients .....: {numClients}");
    Console.WriteLine($"  Successful Connect : {successResults.Count}");
    Console.WriteLine($"  Failed Connect ....: {failResults.Count}");
    Console.WriteLine(thinLine);

    if (successResults.Count > 0)
    {
        var connectTimes = successResults.Select(r => r.ConnectTimeMs).ToList();
        Console.WriteLine("  Connect Time:");
        Console.WriteLine($"    Min .......: {connectTimes.Min():F2} ms");
        Console.WriteLine($"    Max .......: {connectTimes.Max():F2} ms");
        Console.WriteLine($"    Avg .......: {connectTimes.Average():F2} ms");
        Console.WriteLine(thinLine);

        var totalMessages = successResults.Sum(r => r.MessagesSent);
        var bytesSent = successResults.Sum(r => r.BytesSent);
        var bytesRecv = successResults.Sum(r => r.BytesReceived);
        var errors = successResults.Sum(r => r.Errors);

        Console.WriteLine("  Data Transfer:");
        Console.WriteLine($"    Total Msgs ..: {totalMessages:N0}");
        Console.WriteLine($"    Bytes Sent ..: {FormatBytes(bytesSent)}");
        Console.WriteLine($"    Bytes Recv ..: {FormatBytes(bytesRecv)}");
        Console.WriteLine($"    Total I/O ...: {FormatBytes(bytesSent + bytesRecv)}");

        var throughput = (bytesSent + bytesRecv) / stopwatch.Elapsed.TotalSeconds;
        Console.WriteLine($"    Throughput ..: {FormatBytes((long)throughput)}/s");
        Console.WriteLine(thinLine);

        var msgsPerSec = totalMessages / stopwatch.Elapsed.TotalSeconds;
        Console.WriteLine($"    Msg Rate ....: {msgsPerSec:F1} msg/s");
        Console.WriteLine($"    Errors ......: {errors}");
    }
    else
    {
        Console.WriteLine("  No successful connections!");
    }

    Console.WriteLine(separator);

    var passed = successResults.Count >= numClients * 0.9; // 90% threshold
    var passInt = (int)90;
    Console.WriteLine($"  Result: {(passed ? "[PASS]" : "[FAIL]")} {(passed ? $"{successResults.Count}/{numClients} clients connected (> {passInt}%)" : $"{successResults.Count}/{numClients} clients connected (<={passInt}%)")}");
    Console.WriteLine(separator);

    // 詳細失敗原因
    if (failResults.Count > 0 && failResults.Count <= 10)
    {
        Console.WriteLine();
        Console.WriteLine("  Failed Clients:");
        foreach (var f in failResults)
            Console.WriteLine($"    Client #{f.Id}: {f.ErrorMessage}");
    }
    else if (failResults.Count > 10)
    {
        Console.WriteLine($"\n  {failResults.Count} clients failed (showing first 10):");
        foreach (var f in failResults.Take(10))
            Console.WriteLine($"    Client #{f.Id}: {f.ErrorMessage}");
    }
}

static async Task<ClientResult> ClientWorkerAsync(
    int clientId, string host, int port, int durationSec,
    ConcurrentBag<double> connectedTimes)
{
    var result = new ClientResult { Id = clientId };

    try
    {
        using var client = new TcpSocketClient(host, port);

        var connectSw = System.Diagnostics.Stopwatch.StartNew();
        await client.ConnectAsync();
        connectSw.Stop();

        result.ConnectSuccess = true;
        result.ConnectTimeMs = connectSw.ElapsedMilliseconds;
        connectedTimes.Add(connectSw.Elapsed.TotalMilliseconds);

        var receiveBytes = 0L;
        client.DataReceived += (text) =>
        {
            Interlocked.Add(ref receiveBytes, Encoding.UTF8.GetByteCount(text));
        };

        // 計時發送資料
        var payload = $"Hello from Client #{clientId}! [message]";
        var payloadBytes = Encoding.UTF8.GetBytes(payload);
        var sendCount = 0;
        var errorCount = 0;
        long bytesSentAccum = 0;

        var endTime = DateTime.UtcNow.AddSeconds(durationSec);

        while (DateTime.UtcNow < endTime && client.IsConnected)
        {
            try
            {
                await client.SendAsync(payloadBytes);
                Interlocked.Add(ref bytesSentAccum, payloadBytes.Length);
                sendCount++;

                // 每 100 條訊息暫停避免過載
                if (sendCount % 100 == 0)
                    await Task.Delay(1);
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
    }
    catch (Exception ex)
    {
        result.ConnectSuccess = false;
        result.ErrorMessage = ex.Message;
    }

    return result;
}

static string FormatBytes(long bytes) => bytes switch
{
    < 0 => "N/A",
    < 1024 => $"{bytes} B",
    < 1024 * 1024 => $"{bytes / 1024.0:F2} KB",
    < 1024 * 1024 * 1024 => $"{bytes / (1024.0 * 1024):F2} MB",
    _ => $"{bytes / (1024.0 * 1024 * 1024):F2} GB"
};

sealed class ClientResult
{
    public int Id { get; set; }
    public bool ConnectSuccess { get; set; }
    public double ConnectTimeMs { get; set; }
    public string? ErrorMessage { get; set; }
    public int MessagesSent { get; set; }
    public long BytesSent { get; set; }
    public long BytesReceived { get; set; }
    public int Errors { get; set; }
}