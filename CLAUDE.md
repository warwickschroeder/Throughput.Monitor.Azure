# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

A .NET 8 console app that measures ingestion throughput for [Azure DocumentDB](https://azure.microsoft.com/en-gb/products/documentdb) from [Azure Service Bus](https://azure.microsoft.com/en-us/products/service-bus). It polls three Azure services in a loop and outputs a live dashboard with CSV logging.

## Build and Run

```bash
dotnet build
dotnet run -- [serviceBusConnStr] [queueName] [mongoConnStr] [mongoDatabaseName]
```

CLI args are optional and fall back to environment variables:

| Env Var | Required | Default |
|---------|----------|---------|
| `SERVICE_BUS_CONNECTION_STRING` | Yes (if no arg) | — |
| `SERVICE_BUS_QUEUE_NAME` | No | `audit_mongo` |
| `MONGO_CONNECTION_STRING` | Yes (if no arg) | — |
| `MONGO_DATABASE_NAME` | No | `audit` |
| `AZURE_SUBSCRIPTION_ID` | No | hardcoded |
| `AZURE_RESOURCE_GROUP` | No | hardcoded |

Authentication to Azure Monitor uses `DefaultAzureCredential` (requires `az login`).

## Architecture

Single-file app ([Program.cs](Program.cs)) using top-level statements. No tests, no DI, no configuration files — it's a standalone monitoring tool.

**Polling loop** (2s interval, 60min max):

1. **Azure Service Bus** — `ServiceBusAdministrationClient` gets queue runtime properties (active + dead-letter counts)
2. **Azure Monitor** — `MetricsQueryClient` queries IncomingMessages/OutgoingMessages/CompleteMessage metrics every ~60s (30 polls)
3. **Azure DocumentDB** — `MongoDB.Driver` counts documents in `processedMessages` and `messageBodies` collections, runs `ping` and `dbStats` commands

**Derived metrics**: Mongo write rate, body write rate, TTL delete inference (from ack rate minus net doc change), queue depth trend (sliding window), body lag smoothing (60s window).

**Output**: Tabular console output with periodic header reprinting + CSV file written to `throughput-{timestamp}.csv` next to the binary.

**Summary stats** on exit: `MetricAccumulator` tracks min/avg/max/P95 for each metric. `SummaryStats` aggregates all accumulators and threshold breach counts (SLOW, ERR, BACKLOG, DLQ).

## Documentation

When making changes that affect user-facing behavior, configuration, usage, or are otherwise worth noting for users, update [README.md](README.md) alongside the code changes.

## Key Dependencies

- `Azure.Messaging.ServiceBus` — queue administration (not message send/receive)
- `Azure.Monitor.Query` — Azure Monitor metrics API
- `MongoDB.Driver` — Cosmos DB vCore access via MongoDB protocol
- `Azure.Identity` — `DefaultAzureCredential` for Azure Monitor auth
