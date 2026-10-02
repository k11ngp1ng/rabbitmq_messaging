using System.Text.Json;
using Messaging.Contracts;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

var factory = new ConnectionFactory
{
    Uri = new Uri(Environment.GetEnvironmentVariable("RABBITMQ_URI") ?? "amqp://guest:guest@localhost:5672/")
};

try
{
    await using var connection = await factory.CreateConnectionAsync();
    await using var channel = await connection.CreateChannelAsync();
    await DeclareTopologyAsync(channel);
    await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false);

    var consumer = new AsyncEventingBasicConsumer(channel);
    consumer.ReceivedAsync += async (_, delivery) =>
    {
        try
        {
            // The client owns delivery.Body; deserialize before this callback returns.
            var order = JsonSerializer.Deserialize<OrderPlaced>(delivery.Body.Span)
                ?? throw new JsonException("The message body is empty.");
            if (order.OrderId == Guid.Empty || string.IsNullOrWhiteSpace(order.CustomerId) || order.Amount <= 0)
                throw new JsonException("The order is invalid.");

            Console.WriteLine($"Processed order {order.OrderId} for {order.CustomerId} ({order.Amount:F2}).");
        }
        catch (Exception exception)
        {
            Console.Error.WriteLine($"Order processing failed: {exception.Message}");
            await channel.BasicNackAsync(delivery.DeliveryTag, multiple: false, requeue: false);
            return;
        }
        await channel.BasicAckAsync(delivery.DeliveryTag, multiple: false);
    };

    await channel.BasicConsumeAsync(MessagingTopology.Queue, autoAck: false, consumer);
    Console.WriteLine("Waiting for orders. Press Ctrl+C to stop.");
    using var shutdown = new CancellationTokenSource();
    Console.CancelKeyPress += (_, eventArgs) =>
    {
        eventArgs.Cancel = true;
        shutdown.Cancel();
    };
    try { await Task.Delay(Timeout.InfiniteTimeSpan, shutdown.Token); }
    catch (OperationCanceledException) { }
    return 0;
}
catch (Exception exception)
{
    Console.Error.WriteLine($"Consumer failed: {exception.Message}");
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
