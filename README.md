# Throughput.Monitor.Azure

Measure ingestion throughput for [Azure DocumentDB](https://azure.microsoft.com/en-gb/products/documentdb) from [Azure Service Bus](https://azure.microsoft.com/en-us/products/service-bus). Polls queue depth, Azure Monitor metrics, and DocumentDB document counts every 2 seconds, rendering a live console dashboard with CSV export.

## Prerequisites

- [.NET 8 SDK](https://dotnet.microsoft.com/download/dotnet/8.0)
- Azure CLI logged in (`az login`) for Azure Monitor access

## Usage

```bash
dotnet run -- [serviceBusConnStr] [queueName] [mongoConnStr] [mongoDatabaseName]
```

CLI arguments are optional and fall back to environment variables:

| Env Var | Required | Default |
|---------|----------|---------|
| `SERVICE_BUS_CONNECTION_STRING` | Yes (if no arg) | — |
| `SERVICE_BUS_QUEUE_NAME` | No | `audit_mongo` |
| `MONGO_CONNECTION_STRING` | Yes (if no arg) | — |
| `MONGO_DATABASE_NAME` | No | `audit` |
| `AZURE_SUBSCRIPTION_ID` | No | built-in |
| `AZURE_RESOURCE_GROUP` | No | built-in |

## What It Monitors

| Source                | Metrics                                                         |
| --------------------- | --------------------------------------------------------------- |
| **Azure Service Bus** | Active message count, dead-letter queue depth, DLQ growth rate  |
| **Azure Monitor**     | Publish, deliver, and ack rates (msg/s) for the queue           |
| **Azure DocumentDB**  | Document counts, write rates, ping latency, storage/index sizes |

Derived metrics include TTL delete inference, queue depth trend detection, and body lag smoothing.

## Output

- **Console** — live table refreshed every 2s with periodic header reprinting
- **CSV** — `throughput-{timestamp}.csv` written next to the binary
- **Summary** — min/avg/max/P95 stats and threshold breach counts on exit (Ctrl+C or 60min timeout)
