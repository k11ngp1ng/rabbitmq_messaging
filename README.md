# RabbitMQ Messaging with C#

A small, runnable producer/consumer example for an `OrderPlaced` event. It demonstrates durable routing, persistent messages, publisher confirms, manual acknowledgments, prefetch, a dead-letter queue, and idempotent order recording.

## Architecture

```text
Producer -> orders.events (direct exchange) -> orders.processing -> Consumer -> SQLite
                                                     |
                                                     +-- invalid/conflicting message -> orders.dead-letter -> orders.failed
```

The producer publishes one order at a time and waits for broker confirmation. The consumer records an order in SQLite before acknowledging it. `OrderId` is the primary key, so a redelivery cannot create another row. An identical redelivery is acknowledged; a conflicting payload with the same ID is sent to `orders.failed`. Database errors stop the consumer without acknowledging the delivery, allowing RabbitMQ to redeliver it when the channel closes. Delivery remains **at least once**.

## Requirements

- .NET 10 SDK
- Docker with Compose

## Run locally

```powershell
docker compose up -d --wait
$env:RABBITMQ_URI = "amqp://app:localdev@localhost:5672/"
dotnet restore RabbitMqMessaging.slnx --configfile NuGet.Config
dotnet run --project src/Messaging.Consumer
```

In another terminal:

```powershell
$env:RABBITMQ_URI = "amqp://app:localdev@localhost:5672/"
dotnet run --project src/Messaging.Producer -- customer-123 49.90
```

The management UI is at <http://localhost:15672> (`app` / `localdev`). Stop the broker with `docker compose down`; use `docker compose down -v` only when you want to remove local messages too.

The consumer stores orders in `data/orders.db` by default. Set `ORDER_DB_PATH` to another file path if needed. The `data/` directory is excluded from Git. Keep the database across consumer restarts to preserve duplicate detection.

Run the automated idempotency tests with:

```powershell
dotnet test RabbitMqMessaging.slnx
```

## Projects

| Project | Responsibility |
| --- | --- |
| `Messaging.Contracts` | Event schema and topology names |
| `Messaging.Processing` | Atomic order recording and duplicate detection in SQLite |
| `Messaging.Producer` | Validate input, declare topology, publish with confirms |
| `Messaging.Consumer` | Declare topology, consume, acknowledge or dead-letter |
| `Messaging.Processing.Tests` | Verify redelivery and conflicting payload behavior |

Set `RABBITMQ_URI` to point at another broker. The fallback URI uses RabbitMQ's local `guest` credentials for a broker started outside Compose.

## Reliability notes

- A broker confirm means the broker accepted a publish; it does not mean a consumer completed processing.
- Durable queues, a durable exchange, and persistent messages help messages survive broker restarts, subject to broker storage and deployment settings.
- Invalid and conflicting messages go to the dead-letter queue. Inspect and replay them deliberately after fixing the cause.
- The order row and its `OrderId` uniqueness constraint are the same SQLite write, making this example's effect idempotent. Additional effects (payments, emails, or other databases) need their own idempotency mechanism or a transactional outbox.
- SQLite is a local store. Multiple consumers on different hosts need a shared durable database with the same unique constraint.
- For production use, add structured logging, metrics, credentials from a secret store, and a deliberate transient retry policy.

## References

- [RabbitMQ .NET client guide](https://www.rabbitmq.com/client-libraries/dotnet-api-guide)
- [RabbitMQ publisher confirms tutorial](https://www.rabbitmq.com/tutorials/tutorial-seven-dotnet)
