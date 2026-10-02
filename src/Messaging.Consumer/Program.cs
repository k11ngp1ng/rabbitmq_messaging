using System.Text.Json;
using Messaging.Contracts;
using Messaging.Processing;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

var factory = new ConnectionFactory
{
    Uri = new Uri(Environment.GetEnvironmentVariable("RABBITMQ_URI") ?? "amqp://guest:guest@localhost:5672/")
};

try
{
    var store = new OrderStore(Environment.GetEnvironmentVariable("ORDER_DB_PATH") ?? "data/orders.db");
    await store.InitializeAsync();
    await using var connection = await factory.CreateConnectionAsync();
    await using var channel = await connection.CreateChannelAsync();
    await DeclareTopologyAsync(channel);
    await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false);

    using var shutdown = new CancellationTokenSource();
    var processingFailed = 0;
    var consumer = new AsyncEventingBasicConsumer(channel);
    consumer.ReceivedAsync += async (_, delivery) =>
    {
        OrderPlaced order;
        try
        {
            // The client owns delivery.Body; deserialize before this callback returns.
            order = JsonSerializer.Deserialize<OrderPlaced>(delivery.Body.Span)
                ?? throw new JsonException("The message body is empty.");
            if (order.OrderId == Guid.Empty || string.IsNullOrWhiteSpace(order.CustomerId) || order.Amount <= 0)
                throw new JsonException("The order is invalid.");
        }
        catch (JsonException exception)
        {
            Console.Error.WriteLine($"Invalid order message: {exception.Message}");
            await channel.BasicNackAsync(delivery.DeliveryTag, multiple: false, requeue: false);
            return;
        }

        try
        {
            var result = await store.ProcessAsync(order);
            Console.WriteLine($"Order {order.OrderId}: {result}.");
            await channel.BasicAckAsync(delivery.DeliveryTag, multiple: false);
        }
        catch (InvalidDataException exception)
        {
            Console.Error.WriteLine($"Conflicting order message: {exception.Message}");
            await channel.BasicNackAsync(delivery.DeliveryTag, multiple: false, requeue: false);
        }
        catch (Exception exception)
        {
            // Leave the delivery unacknowledged. Closing the channel requeues it.
            Console.Error.WriteLine($"Processing stopped before ACK: {exception.Message}");
            Interlocked.Exchange(ref processingFailed, 1);
            shutdown.Cancel();
        }
    };

    await channel.BasicConsumeAsync(MessagingTopology.Queue, autoAck: false, consumer);
    Console.WriteLine("Waiting for orders. Press Ctrl+C to stop.");
    Console.CancelKeyPress += (_, eventArgs) =>
    {
        eventArgs.Cancel = true;
        shutdown.Cancel();
    };
    try { await Task.Delay(Timeout.InfiniteTimeSpan, shutdown.Token); }
    catch (OperationCanceledException) { }
    return Volatile.Read(ref processingFailed);
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
