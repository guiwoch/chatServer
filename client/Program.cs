using System.Net.WebSockets;
using ChatClient;

const string defaultTarget = "ws://localhost:8080";
var connectTimeout = TimeSpan.FromSeconds(5);

var target = args.Length > 0 ? args[0] : defaultTarget;
if (!Uri.TryCreate(target, UriKind.Absolute, out var uri) || uri.Scheme is not ("ws" or "wss"))
{
    Console.Error.WriteLine($"Invalid target '{target}'. Expected a ws:// or wss:// URL.");
    return 1;
}

using var shutdown = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    if (shutdown.IsCancellationRequested)
        return; // second Ctrl+C: let the runtime terminate us

    e.Cancel = true;
    shutdown.Cancel();
};

using var client = new ClientWebSocket();

try
{
    using var connectCts = CancellationTokenSource.CreateLinkedTokenSource(shutdown.Token);
    connectCts.CancelAfter(connectTimeout);
    await client.ConnectAsync(uri, connectCts.Token);
}
catch (OperationCanceledException) when (shutdown.IsCancellationRequested)
{
    return 0;
}
catch (OperationCanceledException)
{
    Console.Error.WriteLine($"Timed out connecting to {uri} after {connectTimeout.TotalSeconds:0}s.");
    return 1;
}
catch (WebSocketException ex)
{
    Console.Error.WriteLine($"Could not connect to {uri}: {ex.Message}");
    return 1;
}

Console.WriteLine($"-- connected to {uri}. type a message and press enter; ctrl+c to quit.");

await new ChatSession(client).RunAsync(shutdown.Token);
return 0;
