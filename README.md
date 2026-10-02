# RabbitMQ Messaging with C#

A small, runnable producer/consumer example for an `OrderPlaced` event. It demonstrates durable routing, persistent messages, publisher confirms, manual acknowledgments, prefetch, and a dead-letter queue.

## Architecture

```text
Producer -> orders.events (direct exchange) -> orders.processing -> Consumer
                                                     |
                                                     +-- rejected message -> orders.dead-letter -> orders.failed
```

The producer publishes one order at a time and waits for broker confirmation. The consumer processes one unacknowledged message at a time and acknowledges only after successful validation and processing. Invalid messages are rejected into `orders.failed` for inspection. Delivery is **at least once**: an application that performs real side effects must make them idempotent using `OrderId`.

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

## Projects

| Project | Responsibility |
| --- | --- |
| `Messaging.Contracts` | Event schema and topology names |
| `Messaging.Producer` | Validate input, declare topology, publish with confirms |
| `Messaging.Consumer` | Declare topology, consume, acknowledge or dead-letter |

Set `RABBITMQ_URI` to point at another broker. The fallback URI uses RabbitMQ's local `guest` credentials for a broker started outside Compose.

## Reliability notes

- A broker confirm means the broker accepted a publish; it does not mean a consumer completed processing.
- Durable queues, a durable exchange, and persistent messages help messages survive broker restarts, subject to broker storage and deployment settings.
- Invalid messages go to the dead-letter queue. Inspect and replay them deliberately after fixing the cause.
- The sample has no database side effect. For production use, add an idempotent handler, structured logging, metrics, credentials from a secret store, and a deliberate transient retry policy.

## References

- [RabbitMQ .NET client guide](https://www.rabbitmq.com/client-libraries/dotnet-api-guide)
- [RabbitMQ publisher confirms tutorial](https://www.rabbitmq.com/tutorials/tutorial-seven-dotnet)
