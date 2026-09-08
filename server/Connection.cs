using System.Net.WebSockets;
using System.Text;
using System.Threading.Channels;

namespace ChatServer;

class Connection(WebSocket socket)
{
    public async Task HandleAsync(CancellationToken ct)
    {
        using var sub = Hub.Subscribe();
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);

        var sender = SendAsync(sub.Reader, cts.Token);
        var receiver = ReceiveAsync(sub.Id, cts.Token);

        await Task.WhenAny(sender, receiver);
        cts.Cancel();
        await Task.WhenAll(sender, receiver);
    }

    private async Task ReceiveAsync(Guid id, CancellationToken ct)
    {
        try
        {
            var buffer = new byte[4096];
            var memoryStream = new MemoryStream();
            while (true)
            {
                var res = await socket.ReceiveAsync(buffer, ct);

                if (res.MessageType == WebSocketMessageType.Close)
                    break;

                memoryStream.Write(buffer, 0, res.Count);

                if (res.EndOfMessage)
                {
                    var text = Encoding.UTF8.GetString(
                        memoryStream.GetBuffer(),
                        0,
                        (int)memoryStream.Length
                    );
                    Hub.Publish(new Message(id, text, DateTimeOffset.UtcNow));
                    memoryStream.SetLength(0);
                }
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            Console.WriteLine($"ReceiveAsync error: {ex.Message}");
        }
    }

    private async Task SendAsync(ChannelReader<Message> reader, CancellationToken ct)
    {
        try
        {
            await foreach (var msg in reader.ReadAllAsync(ct))
            {
                var bytes = Encoding.UTF8.GetBytes(msg.Text);
                await socket.SendAsync(bytes, WebSocketMessageType.Text, true, ct);
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            Console.WriteLine($"SendAsync error: {ex.Message}");
        }
    }
}
