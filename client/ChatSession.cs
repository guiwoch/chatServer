using System.Net.WebSockets;
using System.Text;
using System.Threading.Channels;

namespace ChatClient;

sealed class ChatSession(ClientWebSocket socket)
{
    public async Task RunAsync(CancellationToken ct)
    {
        using var sendCts = CancellationTokenSource.CreateLinkedTokenSource(ct);

        var receiver = ReceiveLoopAsync();
        var sender = SendLoopAsync(sendCts.Token);

        await Task.WhenAny(receiver, sender);

        await sendCts.CancelAsync();
        await sender;

        await CloseOutputAsync();

        try
        {
            await receiver.WaitAsync(TimeSpan.FromSeconds(5));
        }
        catch (TimeoutException) { }
    }

    private async Task ReceiveLoopAsync()
    {
        var buffer = new byte[4096];
        using var message = new MemoryStream();

        try
        {
            while (true)
            {
                var res = await socket.ReceiveAsync(
                    new ArraySegment<byte>(buffer),
                    CancellationToken.None
                );

                if (res.MessageType == WebSocketMessageType.Close)
                {
                    Console.WriteLine(
                        $"-- closed ({res.CloseStatus}) {res.CloseStatusDescription}"
                    );
                    return;
                }

                message.Write(buffer, 0, res.Count);

                if (!res.EndOfMessage)
                    continue;

                Console.WriteLine(
                    Encoding.UTF8.GetString(message.GetBuffer(), 0, (int)message.Length)
                );
                message.SetLength(0);
            }
        }
        catch (OperationCanceledException) { }
        catch (WebSocketException ex)
        {
            Console.Error.WriteLine($"-- connection lost: {ex.Message}");
        }
    }

    private async Task SendLoopAsync(CancellationToken ct)
    {
        var lines = StartStdinReader();

        try
        {
            await foreach (var line in lines.ReadAllAsync(ct))
            {
                if (socket.State != WebSocketState.Open)
                    return;

                var bytes = Encoding.UTF8.GetBytes(line);

                await socket.SendAsync(
                    bytes,
                    WebSocketMessageType.Text,
                    endOfMessage: true,
                    CancellationToken.None
                );
            }
        }
        catch (OperationCanceledException) { }
        catch (WebSocketException ex)
        {
            Console.Error.WriteLine($"-- send failed: {ex.Message}");
        }
    }

    private static ChannelReader<string> StartStdinReader()
    {
        var channel = Channel.CreateUnbounded<string>(
            new UnboundedChannelOptions { SingleReader = true, SingleWriter = true }
        );

        var thread = new Thread(() =>
        {
            try
            {
                // ReadLine returns null on EOF (Ctrl+D, or a closed pipe).
                while (Console.ReadLine() is { } line && channel.Writer.TryWrite(line)) { }
            }
            finally
            {
                channel.Writer.TryComplete();
            }
        })
        {
            IsBackground = true,
            Name = "stdin",
        };

        thread.Start();
        return channel.Reader;
    }

    private async Task CloseOutputAsync()
    {
        if (socket.State is not (WebSocketState.Open or WebSocketState.CloseReceived))
            return;

        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await socket.CloseOutputAsync(
                WebSocketCloseStatus.NormalClosure,
                "Client leaving",
                timeout.Token
            );
        }
        catch (WebSocketException) { }
        catch (OperationCanceledException) { }
    }
}
