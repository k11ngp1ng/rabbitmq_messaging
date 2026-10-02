using System.Globalization;
using System.Text.Json;
using Messaging.Contracts;
using RabbitMQ.Client;

if (args.Length != 2 || string.IsNullOrWhiteSpace(args[0]) ||
    !decimal.TryParse(args[1], NumberStyles.Number, CultureInfo.InvariantCulture, out var amount) || amount <= 0)
{
    Console.Error.WriteLine("Usage: dotnet run --project src/Messaging.Producer -- <customer-id> <positive-amount>");
    return 2;
}

var order = new OrderPlaced(Guid.NewGuid(), args[0], amount, DateTimeOffset.UtcNow);
var factory = new ConnectionFactory
{
    Uri = new Uri(Environment.GetEnvironmentVariable("RABBITMQ_URI") ?? "amqp://guest:guest@localhost:5672/")
};

try
{
    await using var connection = await factory.CreateConnectionAsync();
    await using var channel = await connection.CreateChannelAsync(new CreateChannelOptions(
        publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true));
    await DeclareTopologyAsync(channel);

    var properties = new BasicProperties
    {
        Persistent = true,
        ContentType = "application/json",
        MessageId = order.OrderId.ToString(),
        Type = nameof(OrderPlaced)
    };
    using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
    await channel.BasicPublishAsync(MessagingTopology.Exchange, MessagingTopology.RoutingKey,
        mandatory: true, basicProperties: properties, body: JsonSerializer.SerializeToUtf8Bytes(order),
        cancellationToken: timeout.Token);
    Console.WriteLine($"Published order {order.OrderId} for {order.CustomerId} ({order.Amount:F2}).");
    return 0;
}
catch (Exception exception)
{
    Console.Error.WriteLine($"Publishing failed: {exception.Message}");
    return 1;
}

static async Task DeclareTopologyAsync(IChannel channel)
{
    await channel.ExchangeDeclareAsync(MessagingTopology.Exchange, ExchangeType.Direct, durable: true);
    await channel.ExchangeDeclareAsync(MessagingTopology.DeadLetterExchange, ExchangeType.Direct, durable: true);
    await channel.QueueDeclareAsync(MessagingTopology.DeadLetterQueue, durable: true,
        exclusive: false, autoDelete: false, arguments: null);
    await channel.QueueBindAsync(MessagingTopology.DeadLetterQueue,
        MessagingTopology.DeadLetterExchange, MessagingTopology.RoutingKey);
    await channel.QueueDeclareAsync(MessagingTopology.Queue, durable: true, exclusive: false,
        autoDelete: false, arguments: new Dictionary<string, object?>
        {
            ["x-dead-letter-exchange"] = MessagingTopology.DeadLetterExchange
        });
    await channel.QueueBindAsync(MessagingTopology.Queue, MessagingTopology.Exchange,
        MessagingTopology.RoutingKey);
}
