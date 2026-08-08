using System.Text;
using SocketClientLib;

Console.Title = "Socket Client";

await RunClientAsync();

static async Task RunClientAsync()
{
    var host = Environment.GetEnvironmentVariable("SERVER_HOST") ?? "127.0.0.1";
    var port = int.TryParse(Environment.GetEnvironmentVariable("SERVER_PORT"), out var p) ? p : 5000;
    var command = Environment.GetEnvironmentVariable("COMMAND");

    Console.WriteLine($"Connecting to {host}:{port}...");

    try
    {
        using var client = new TcpSocketClient(host, port);

        client.Connected += () => Console.WriteLine("Connected!");
        client.Disconnected += () => Console.WriteLine("Disconnected by server.");

        await client.ConnectAsync();

        if (!string.IsNullOrEmpty(command))
        {
            // 指令模式：發送指定指令並接收回應
            var response = await client.SendCommandAsync(command);
            Console.WriteLine("[Response]:");
            Console.Write(response);
        }
        else
        {
            // 互動模式
            await InteractiveModeAsync(client);
        }

        Console.WriteLine("Disconnected.");
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine($"Connection error: {ex.Message}");
        Environment.Exit(1);
    }
}

static async Task InteractiveModeAsync(TcpSocketClient client)
{
    Console.WriteLine("Interactive mode. Type 'quit' to exit.");
    Console.WriteLine("Available commands: ping, stats, clients, disconnect");

    var receivedData = new StringBuilder();
    var dataLock = new object();

    // 訂閱資料接收事件
    client.DataReceived += (text) =>
    {
        lock (dataLock)
        {
            receivedData.Append(text);
        }
    };

    try
    {
        while (client.IsConnected)
        {
            Console.Write("\n> ");
            var input = Console.ReadLine();

            // 輸出累積的回應
            lock (dataLock)
            {
                if (receivedData.Length > 0)
                {
                    Console.Write(receivedData.ToString());
                    receivedData.Clear();
                }
            }

            if (string.IsNullOrEmpty(input)) continue;

            if (input.Trim().ToLowerInvariant() == "quit")
                break;

            await client.SendTextAsync(input);
        }
    }
    finally
    {
        client.Disconnect();
    }
}