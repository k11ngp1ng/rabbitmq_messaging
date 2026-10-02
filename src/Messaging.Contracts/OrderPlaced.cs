namespace Messaging.Contracts;

public sealed record OrderPlaced(Guid OrderId, string CustomerId, decimal Amount, DateTimeOffset OccurredAtUtc);

public static class MessagingTopology
{
    public const string Exchange = "orders.events";
    public const string RoutingKey = "order.placed";
    public const string Queue = "orders.processing";
    public const string DeadLetterExchange = "orders.dead-letter";
    public const string DeadLetterQueue = "orders.failed";
}
