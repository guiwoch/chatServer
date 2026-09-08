using System.Net.WebSockets;
using ChatServer;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddHostedService<Broker>();
var app = builder.Build();

app.UseWebSockets();

app.Run(async context =>
{
    if (!context.WebSockets.IsWebSocketRequest)
    {
        context.Response.StatusCode = 400;
        return;
    }
    using var socket = await context.WebSockets.AcceptWebSocketAsync();
    try
    {
        var conn = new Connection(socket);
        await conn.HandleAsync(context.RequestAborted);
    }
    finally
    {
        var ct = new CancellationTokenSource(TimeSpan.FromSeconds(5)).Token;
        await socket.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, "Closing", ct);
    }
});

app.Run();

sealed class Broker : BackgroundService
{
    protected override Task ExecuteAsync(CancellationToken ct) => Hub.RunAsync(ct);
}
