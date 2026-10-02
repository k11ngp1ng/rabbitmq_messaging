using System.Globalization;
using Messaging.Contracts;
using Microsoft.Data.Sqlite;

namespace Messaging.Processing;

public enum ProcessingResult
{
    Processed,
    AlreadyProcessed
}

public sealed class OrderStore(string databasePath)
{
    private readonly string _databasePath = Path.GetFullPath(databasePath);

    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(_databasePath)!);
        await using var connection = await OpenConnectionAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            CREATE TABLE IF NOT EXISTS processed_orders (
                order_id TEXT PRIMARY KEY,
                customer_id TEXT NOT NULL,
                amount TEXT NOT NULL,
                occurred_at_utc TEXT NOT NULL,
                processed_at_utc TEXT NOT NULL
            );
            """;
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<ProcessingResult> ProcessAsync(OrderPlaced order, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(order);
        if (order.OrderId == Guid.Empty || string.IsNullOrWhiteSpace(order.CustomerId) || order.Amount <= 0)
            throw new ArgumentException("The order is invalid.", nameof(order));

        await using var connection = await OpenConnectionAsync(cancellationToken);
        await using var insert = connection.CreateCommand();
        insert.CommandText = """
            INSERT INTO processed_orders (order_id, customer_id, amount, occurred_at_utc, processed_at_utc)
            VALUES ($orderId, $customerId, $amount, $occurredAt, $processedAt)
            ON CONFLICT(order_id) DO NOTHING;
            """;
        insert.Parameters.AddWithValue("$orderId", order.OrderId.ToString());
        insert.Parameters.AddWithValue("$customerId", order.CustomerId);
        insert.Parameters.AddWithValue("$amount", order.Amount.ToString(CultureInfo.InvariantCulture));
        insert.Parameters.AddWithValue("$occurredAt", order.OccurredAtUtc.ToString("O"));
        insert.Parameters.AddWithValue("$processedAt", DateTimeOffset.UtcNow.ToString("O"));

        if (await insert.ExecuteNonQueryAsync(cancellationToken) == 1)
            return ProcessingResult.Processed;

        await using var read = connection.CreateCommand();
        read.CommandText = """
            SELECT customer_id, amount, occurred_at_utc
            FROM processed_orders WHERE order_id = $orderId;
            """;
        read.Parameters.AddWithValue("$orderId", order.OrderId.ToString());
        await using var reader = await read.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
            throw new InvalidOperationException("The duplicate order could not be read.");

        if (reader.GetString(0) != order.CustomerId ||
            reader.GetString(1) != order.Amount.ToString(CultureInfo.InvariantCulture) ||
            reader.GetString(2) != order.OccurredAtUtc.ToString("O"))
            throw new InvalidDataException("The same OrderId was reused with different order details.");

        return ProcessingResult.AlreadyProcessed;
    }

    private async Task<SqliteConnection> OpenConnectionAsync(CancellationToken cancellationToken)
    {
        var connection = new SqliteConnection(new SqliteConnectionStringBuilder
        {
            DataSource = _databasePath,
            Mode = SqliteOpenMode.ReadWriteCreate
        }.ToString());
        try
        {
            await connection.OpenAsync(cancellationToken);
            return connection;
        }
        catch
        {
            await connection.DisposeAsync();
            throw;
        }
    }
}
