using System.Collections.Concurrent;
using System.Threading.Channels;

namespace ChatServer;

record Message(Guid Sender, string Text, DateTimeOffset At);

sealed class Subscription(Guid id, ChannelReader<Message> reader) : IDisposable
{
    public Guid Id { get; } = id;
    public ChannelReader<Message> Reader { get; } = reader;

    public void Dispose()
    {
        Hub.Unsubscribe(Id);
    }
}

static class Hub
{
    private static readonly Channel<Message> _hub = Channel.CreateUnbounded<Message>();
    private static readonly ConcurrentDictionary<Guid, Channel<Message>> _clients = new();

    public static void Publish(Message message)
    {
        _hub.Writer.TryWrite(message);
    }

    public static Subscription Subscribe()
    {
        var id = Guid.NewGuid();
        var channel = Channel.CreateUnbounded<Message>();
        _clients[id] = channel;
        return new Subscription(id, channel.Reader);
    }

    public static void Unsubscribe(Guid id)
    {
        if (_clients.TryRemove(id, out var chan))
            chan.Writer.Complete();
    }

    public static async Task RunAsync(CancellationToken ct)
    {
        await foreach (var msg in _hub.Reader.ReadAllAsync(ct))
        {
            foreach (var (clientId, chan) in _clients)
            {
                if (clientId == msg.Sender)
                    continue;

                chan.Writer.TryWrite(msg);
            }
        }
    }
}
