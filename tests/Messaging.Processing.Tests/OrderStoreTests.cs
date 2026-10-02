using Messaging.Contracts;
using Messaging.Processing;
using Microsoft.Data.Sqlite;
using Xunit;

namespace Messaging.Processing.Tests;

public sealed class OrderStoreTests
{
    [Fact]
    public async Task Redelivery_DoesNotCreateAnotherOrder()
    {
        var databasePath = NewDatabasePath();
        var order = new OrderPlaced(Guid.NewGuid(), "customer-123", 49.90m, DateTimeOffset.UtcNow);
        var firstStore = new OrderStore(databasePath);
        await firstStore.InitializeAsync();

        Assert.Equal(ProcessingResult.Processed, await firstStore.ProcessAsync(order));
        var restartedStore = new OrderStore(databasePath);
        await restartedStore.InitializeAsync();
        Assert.Equal(ProcessingResult.AlreadyProcessed, await restartedStore.ProcessAsync(order));

        await using var connection = new SqliteConnection($"Data Source={databasePath}");
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM processed_orders;";
        Assert.Equal(1L, await command.ExecuteScalarAsync());
    }

    [Fact]
    public async Task ReusedOrderIdWithDifferentPayload_IsRejected()
    {
        var store = new OrderStore(NewDatabasePath());
        await store.InitializeAsync();
        var original = new OrderPlaced(Guid.NewGuid(), "customer-123", 49.90m, DateTimeOffset.UtcNow);
        await store.ProcessAsync(original);

        await Assert.ThrowsAsync<InvalidDataException>(() =>
            store.ProcessAsync(original with { Amount = 99.90m }));
    }

    private static string NewDatabasePath() =>
        Path.Combine(Path.GetTempPath(), "rabbitmq-messaging-tests", Guid.NewGuid().ToString("N"), "orders.db");
}
