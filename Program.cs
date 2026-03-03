using System.Diagnostics;
using Azure.Identity;
using Azure.Messaging.ServiceBus.Administration;
using Azure.Monitor.Query;
using Azure.Monitor.Query.Models;
using MongoDB.Bson;
using MongoDB.Driver;

// ============================================================================
// Configuration - adjust these to match your Azure environment
// ============================================================================

// Azure Service Bus
var serviceBusConnectionString = args.Length > 0 ? args[0]
    : Environment.GetEnvironmentVariable("SERVICE_BUS_CONNECTION_STRING")
      ?? throw new InvalidOperationException("Provide Service Bus connection string as arg[0] or SERVICE_BUS_CONNECTION_STRING env var");
var queueName = args.Length > 1 ? args[1]
    : Environment.GetEnvironmentVariable("SERVICE_BUS_QUEUE_NAME") ?? "audit_mongo";

// Azure DocumentDB (Cosmos DB for MongoDB vCore)
var mongoConnectionString = args.Length > 2 ? args[2]
    : Environment.GetEnvironmentVariable("MONGO_CONNECTION_STRING")
      ?? throw new InvalidOperationException("Provide MongoDB connection string as arg[2] or MONGO_CONNECTION_STRING env var");
var mongoDatabaseName = args.Length > 3 ? args[3]
    : Environment.GetEnvironmentVariable("MONGO_DATABASE_NAME") ?? "audit";
var collectionName = "processedMessages";

// Azure Monitor (for Service Bus publish/deliver/ack metrics)
var subscriptionId = Environment.GetEnvironmentVariable("AZURE_SUBSCRIPTION_ID") ?? "934f5a76-bd9e-4d9a-be26-94b1476bab33";
var resourceGroup = Environment.GetEnvironmentVariable("AZURE_RESOURCE_GROUP") ?? "tf-cloudxp-sc_cloud_storage-679";
var serviceBusNamespace = GetNamespaceFromConnectionString(serviceBusConnectionString);
var serviceBusResourceId = $"/subscriptions/{subscriptionId}/resourceGroups/{resourceGroup}/providers/Microsoft.ServiceBus/namespaces/{serviceBusNamespace}";

var pollIntervalMs = 2000;
var maxRunTime = TimeSpan.FromMinutes(60);
const int metricsQueryInterval = 30; // Query Azure Monitor every 30 polls (~60s)

// ============================================================================

// Setup Azure Service Bus admin client
var sbAdmin = new ServiceBusAdministrationClient(serviceBusConnectionString);

// Setup Azure Monitor metrics client (uses az login / DefaultAzureCredential)
var metricsClient = new MetricsQueryClient(new DefaultAzureCredential());

// Setup MongoDB client
var mongoClient = new MongoClient(mongoConnectionString);
var db = mongoClient.GetDatabase(mongoDatabaseName);
var collection = db.GetCollection<BsonDocument>(collectionName);
var bodiesCollection = db.GetCollection<BsonDocument>("messageBodies");

// State for delta calculations
long? prevMongoCount = null;
long? prevBodyCount = null;
long? prevDeadLetterCount = null;
DateTime prevTime = DateTime.UtcNow;
int rowCount = 0;
const int headerRepeatInterval = 20;

// Cached Azure Monitor rates (updated every ~60s, 1-minute resolution)
double cachedPublishRate = 0;
double cachedDeliverRate = 0;
double cachedAckRate = 0;

// Queue depth trend tracking
const int trendWindowSize = 15; // 30 seconds at 2s intervals
var queueDepthWindow = new Queue<long>();
long? prevQueueDepth = null;
int consecutiveGrowth = 0;

// Body lag smoothing (60s window = 30 samples at 2s intervals)
const int bodyLagSmoothingWindow = 30;
var bodyLagWindow = new Queue<long>();

// Summary stats
var summaryStats = new SummaryStats();

// CSV log file
var logPath = Path.Combine(AppContext.BaseDirectory, $"throughput-{DateTime.Now:yyyyMMdd-HHmmss}.csv");
await using var logWriter = new StreamWriter(logPath, append: false);
await logWriter.WriteLineAsync("Timestamp,ASB_Publish_MsgSec,ASB_Deliver_MsgSec,ASB_Ack_MsgSec,Queue_Active,Queue_DeadLetter,DLQ_Rate,Mongo_Docs,Mongo_MsgSec,Body_MsgSec,Body_Lag,TTL_Del_Sec,Ping_Ms,Data_MB,Storage_MB,Index_MB,Queue_Trend,Status");

Console.WriteLine($"Azure Throughput Monitor - logging to {logPath}");
Console.WriteLine($"Service Bus: {serviceBusNamespace}.servicebus.windows.net queue={queueName}");
Console.WriteLine($"Azure Monitor: {serviceBusResourceId}");
Console.WriteLine($"DocumentDB: {mongoDatabaseName}.{collectionName}");
Console.WriteLine($"Max run: {maxRunTime.TotalMinutes}min  |  Poll: {pollIntervalMs}ms  |  ASB metrics: every {metricsQueryInterval * pollIntervalMs / 1000}s");
Console.WriteLine();
PrintHeaders();

var startTime = DateTime.UtcNow;
using var cts = new CancellationTokenSource(maxRunTime);

Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

try
{

while (!cts.Token.IsCancellationRequested)
{
    try
    {
        var now = DateTime.UtcNow;
        var elapsed = (now - prevTime).TotalSeconds;

        // --- Azure Service Bus queue metrics ---
        long activeMessages = 0;
        long deadLetterCount = 0;

        try
        {
            var props = await sbAdmin.GetQueueRuntimePropertiesAsync(queueName, cts.Token);
            activeMessages = props.Value.ActiveMessageCount;
            deadLetterCount = props.Value.DeadLetterMessageCount;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"  Service Bus error: {ex.Message}");
        }

        // --- Azure Monitor: publish/deliver/ack rates (every ~60s) ---
        if (rowCount % metricsQueryInterval == 0)
        {
            try
            {
                var result = await metricsClient.QueryResourceAsync(
                    serviceBusResourceId,
                    new[] { "IncomingMessages", "OutgoingMessages", "CompleteMessage" },
                    new MetricsQueryOptions
                    {
                        TimeRange = new QueryTimeRange(TimeSpan.FromMinutes(5)),
                        Granularity = TimeSpan.FromMinutes(1),
                        Filter = $"EntityName eq '{queueName}'"
                    },
                    cts.Token);

                foreach (var metric in result.Value.Metrics)
                {
                    var latest = metric.TimeSeries.FirstOrDefault()?.Values
                        .Where(v => v.Total.HasValue)
                        .LastOrDefault();

                    if (latest?.Total is { } total)
                    {
                        var rate = total / 60.0;
                        switch (metric.Name)
                        {
                            case "IncomingMessages": cachedPublishRate = rate; break;
                            case "OutgoingMessages": cachedDeliverRate = rate; break;
                            case "CompleteMessage": cachedAckRate = rate; break;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"  Azure Monitor error: {ex.Message}");
            }
        }

        // --- MongoDB doc counts (accurate countDocuments, not EstimatedDocumentCount) ---
        // EstimatedDocumentCount on Cosmos DB uses cached metadata that can swing wildly.
        // countDocuments({}) scans the _id index and gives a stable, accurate count.
        long mongoCount = 0;
        try
        {
            mongoCount = await collection.CountDocumentsAsync(FilterDefinition<BsonDocument>.Empty, cancellationToken: cts.Token);
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"  DocumentDB audit count error: {ex.Message}");
        }

        long bodyCount = 0;
        try
        {
            bodyCount = await bodiesCollection.CountDocumentsAsync(FilterDefinition<BsonDocument>.Empty, cancellationToken: cts.Token);
        }
        catch
        {
            // Body collection may not exist if BodyStorageType=None
        }

        // --- DocumentDB health: ping latency + storage ---
        double pingMs = 0;
        double dataMb = 0;
        double storageMb = 0;
        double indexMb = 0;
        var status = "OK";

        try
        {
            // Ping latency
            var sw = Stopwatch.StartNew();
            await db.RunCommandAsync<BsonDocument>(new BsonDocument("ping", 1), cancellationToken: cts.Token);
            sw.Stop();
            pingMs = sw.Elapsed.TotalMilliseconds;

            if (pingMs >= 500)
            {
                status = "SLOW!";
            }

            // Database storage size
            try
            {
                var dbStats = await db.RunCommandAsync<BsonDocument>(new BsonDocument("dbStats", 1), cancellationToken: cts.Token);
                if (dbStats.Contains("dataSize")) dataMb = dbStats["dataSize"].ToDouble() / (1024 * 1024);
                if (dbStats.Contains("storageSize")) storageMb = dbStats["storageSize"].ToDouble() / (1024 * 1024);
                if (dbStats.Contains("indexSize")) indexMb = dbStats["indexSize"].ToDouble() / (1024 * 1024);
            }
            catch
            {
                // dbStats may not be supported on all DocumentDB tiers
            }
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"  DocumentDB health error: {ex.Message}");
            status = "ERR";
        }

        // --- Output ---
        if (prevMongoCount.HasValue && elapsed > 0)
        {
            // Infer TTL deletes: ASB ack rate (true insert rate) minus net doc count change
            var netDocChange = mongoCount - prevMongoCount.Value;
            var expectedInserts = cachedAckRate * elapsed;
            var inferredTtlDeletes = Math.Max(0, expectedInserts - netDocChange);
            var inferredTtlDelPerSec = inferredTtlDeletes / elapsed;

            var mongoRate = Math.Max(0, netDocChange + inferredTtlDeletes) / elapsed;
            var bodyRate = prevBodyCount.HasValue ? (bodyCount - prevBodyCount.Value) / elapsed : 0;

            // DLQ growth rate
            var dlqRate = prevDeadLetterCount.HasValue ? (deadLetterCount - prevDeadLetterCount.Value) / elapsed : 0;

            // Smooth body lag
            var rawBodyLag = bodyCount == 0 && bodyRate == 0 ? 0 : mongoCount - bodyCount;
            bodyLagWindow.Enqueue(rawBodyLag);
            while (bodyLagWindow.Count > bodyLagSmoothingWindow) bodyLagWindow.Dequeue();
            var bodyLag = (long)bodyLagWindow.Average();

            var timestamp = DateTime.Now.ToString("HH:mm:ss.f");

            // Queue depth trend detection
            if (prevQueueDepth.HasValue)
            {
                if (activeMessages > prevQueueDepth.Value)
                    consecutiveGrowth++;
                else if (activeMessages < prevQueueDepth.Value)
                    consecutiveGrowth = Math.Min(consecutiveGrowth - 1, 0);
                else
                    consecutiveGrowth = 0;
            }
            prevQueueDepth = activeMessages;

            // Sliding window for trend slope
            queueDepthWindow.Enqueue(activeMessages);
            while (queueDepthWindow.Count > trendWindowSize) queueDepthWindow.Dequeue();

            double queueGrowthRate = 0;
            if (queueDepthWindow.Count >= 2)
            {
                var windowItems = queueDepthWindow.ToArray();
                queueGrowthRate = (windowItems[^1] - windowItems[0]) / ((queueDepthWindow.Count - 1) * (pollIntervalMs / 1000.0));
            }

            var trend = consecutiveGrowth >= 5 ? "BACKLOG"
                      : consecutiveGrowth >= 2 ? "growing"
                      : consecutiveGrowth <= -2 ? "drain"
                      : "stable";

            if (consecutiveGrowth >= 5 && status == "OK")
            {
                status = "BACKLOG";
            }

            if (dlqRate > 0 && status == "OK")
            {
                status = "DLQ!";
            }

            summaryStats.Record(cachedPublishRate, cachedDeliverRate, cachedAckRate,
                activeMessages, mongoRate, bodyRate, bodyLag, inferredTtlDelPerSec, dlqRate,
                pingMs, dataMb, storageMb, indexMb, queueGrowthRate, status);

            if (rowCount > 0 && rowCount % headerRepeatInterval == 0)
            {
                PrintHeaders();
            }
            rowCount++;

            Console.WriteLine(
                $"{timestamp,-12} | {cachedPublishRate,8:F1} | {cachedDeliverRate,8:F1} | {cachedAckRate,8:F1} | {activeMessages,10:N0} | {deadLetterCount,8:N0} | {dlqRate,8:F1} | {mongoCount,10:N0} | {mongoRate,8:F1} | {bodyRate,8:F1} | {bodyLag,8:N0} | {inferredTtlDelPerSec,8:F0} | {pingMs,6:F0}ms | {storageMb,8:F0}MB | {trend,7} | {status}");

            await logWriter.WriteLineAsync(
                $"{DateTime.Now:O},{cachedPublishRate:F1},{cachedDeliverRate:F1},{cachedAckRate:F1},{activeMessages},{deadLetterCount},{dlqRate:F1},{mongoCount},{mongoRate:F1},{bodyRate:F1},{bodyLag},{inferredTtlDelPerSec:F0},{pingMs:F0},{dataMb:F1},{storageMb:F1},{indexMb:F1},{queueGrowthRate:F1},{status}");
            await logWriter.FlushAsync();
        }
        else
        {
            Console.WriteLine($"{"Warming up...",-12} | {"---",8} | {"---",8} | {"---",8} | {activeMessages,10:N0} | {deadLetterCount,8:N0} | {"---",8} | {mongoCount,10:N0} | {"---",8} | {"---",8} | {"---",8} | {"---",8} | {pingMs,6:F0}ms | {storageMb,8:F0}MB | {"",7} | {status}");
        }

        prevMongoCount = mongoCount;
        prevBodyCount = bodyCount;
        prevDeadLetterCount = deadLetterCount;
        prevTime = now;

        await Task.Delay(pollIntervalMs, cts.Token);
    }
    catch (OperationCanceledException)
    {
        break;
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine($"Error: {ex.Message}");
        await Task.Delay(pollIntervalMs);
    }
}
}
finally
{

// --- Summary ---
var totalElapsed = DateTime.UtcNow - startTime;
Console.WriteLine();
Console.WriteLine(new string('=', 120));
Console.WriteLine($"  SUMMARY  |  Duration: {totalElapsed:m\\:ss}  |  Samples: {summaryStats.Count}  |  Log: {logPath}");
Console.WriteLine(new string('=', 120));

if (summaryStats.Count > 0)
{
    Console.WriteLine();
    Console.WriteLine($"  {"Metric",-24} | {"Avg",10} | {"Min",10} | {"Max",10} | {"P95",10}");
    Console.WriteLine($"  {new string('-', 22)} | {new string('-', 10)} | {new string('-', 10)} | {new string('-', 10)} | {new string('-', 10)}");
    PrintStat("ASB Publish (msg/s)", summaryStats.Publish);
    PrintStat("ASB Deliver (msg/s)", summaryStats.Deliver);
    PrintStat("ASB Ack (msg/s)", summaryStats.Ack);
    PrintStat("Queue Depth", summaryStats.QueueDepth);
    PrintStat("Mongo Stored (msg/s)", summaryStats.MongoRate);
    PrintStat("Body Written (msg/s)", summaryStats.BodyRate);
    PrintStat("Body Lag (docs)", summaryStats.BodyLag);
    PrintStat("TTL Deletes (del/s)", summaryStats.TtlDel);
    PrintStat("DLQ Rate (msg/s)", summaryStats.DlqRate);
    PrintStat("Ping Latency (ms)", summaryStats.Ping);
    PrintStat("Queue Growth (msg/s)", summaryStats.QueueGrowthRate);

    Console.WriteLine();
    Console.WriteLine($"  Storage: Data={summaryStats.DataMb.Max:F1}MB  Storage={summaryStats.StorageMb.Max:F1}MB  Indexes={summaryStats.IndexMb.Max:F1}MB");
    Console.WriteLine($"  Threshold breaches: SLOW={summaryStats.SlowCount}  ERR={summaryStats.ErrorCount}  BACKLOG={summaryStats.BacklogCount}  DLQ={summaryStats.DlqCount}");

    if (summaryStats.BacklogCount > 0)
    {
        var backlogPct = (double)summaryStats.BacklogCount / summaryStats.Count * 100;
        Console.WriteLine();
        Console.WriteLine($"  ** ServiceControl could NOT keep up for {backlogPct:F0}% of the test ({summaryStats.BacklogCount}/{summaryStats.Count} samples)");
        Console.WriteLine($"  ** Peak queue growth: {summaryStats.QueueGrowthRate.Max:F1} msg/s faster than consumption");
    }
    else if (summaryStats.QueueDepth.Max > 100 && summaryStats.QueueDepth.Avg > 50)
    {
        Console.WriteLine();
        Console.WriteLine($"  ** ServiceControl kept up but queue depth was elevated (avg={summaryStats.QueueDepth.Avg:F0}, max={summaryStats.QueueDepth.Max:F0})");
    }
    else
    {
        Console.WriteLine();
        Console.WriteLine($"  ** ServiceControl kept up with the load");
    }

    if (summaryStats.DlqCount > 0)
    {
        Console.WriteLine($"  ** WARNING: {summaryStats.DlqCount} samples had dead-letter queue growth - check for poison messages");
    }
}

Console.WriteLine();

} // finally

static string GetNamespaceFromConnectionString(string cs)
{
    var parts = cs.Split(';');
    foreach (var part in parts)
    {
        if (part.TrimStart().StartsWith("Endpoint=", StringComparison.OrdinalIgnoreCase))
        {
            var host = new Uri(part.Split('=', 2)[1]).Host;
            return host.Split('.')[0];
        }
    }
    return "";
}

static void PrintStat(string name, MetricAccumulator m)
{
    Console.WriteLine($"  {name,-24} | {m.Avg,10:F1} | {m.Min,10:F1} | {m.Max,10:F1} | {m.P95,10:F1}");
}

static void PrintHeaders()
{
    Console.WriteLine($"{"Time",-12} | {"Publish",8} | {"Deliver",8} | {"Ack",8} | {"Queue",10} | {"DLQ",8} | {"DLQ",8} | {"Mongo",10} | {"Stored",8} | {"Body",8} | {"BLag",8} | {"TTL Del",8} | {"Ping",8} | {"Disk",8} | {"Trend",7} |");
    Console.WriteLine($"{"",12} | {"msg/s",8} | {"msg/s",8} | {"msg/s",8} | {"active",10} | {"msgs",8} | {"msg/s",8} | {"docs",10} | {"msg/s",8} | {"msg/s",8} | {"docs",8} | {"del/s",8} | {"",8} | {"",8} | {"",7} |");
    Console.WriteLine(new string('-', 181));
}

class MetricAccumulator
{
    readonly List<double> values = [];
    public double Min { get; private set; } = double.MaxValue;
    public double Max { get; private set; } = double.MinValue;
    double sum;

    public void Add(double value)
    {
        values.Add(value);
        sum += value;
        if (value < Min) Min = value;
        if (value > Max) Max = value;
    }

    public double Avg => values.Count > 0 ? sum / values.Count : 0;

    public double P95
    {
        get
        {
            if (values.Count == 0) return 0;
            var sorted = values.OrderBy(v => v).ToList();
            var index = (int)Math.Ceiling(sorted.Count * 0.95) - 1;
            return sorted[Math.Max(0, index)];
        }
    }
}

class SummaryStats
{
    public int Count { get; private set; }
    public MetricAccumulator Publish { get; } = new();
    public MetricAccumulator Deliver { get; } = new();
    public MetricAccumulator Ack { get; } = new();
    public MetricAccumulator QueueDepth { get; } = new();
    public MetricAccumulator MongoRate { get; } = new();
    public MetricAccumulator BodyRate { get; } = new();
    public MetricAccumulator BodyLag { get; } = new();
    public MetricAccumulator TtlDel { get; } = new();
    public MetricAccumulator DlqRate { get; } = new();
    public MetricAccumulator Ping { get; } = new();
    public MetricAccumulator DataMb { get; } = new();
    public MetricAccumulator StorageMb { get; } = new();
    public MetricAccumulator IndexMb { get; } = new();
    public MetricAccumulator QueueGrowthRate { get; } = new();
    public int SlowCount { get; private set; }
    public int ErrorCount { get; private set; }
    public int BacklogCount { get; private set; }
    public int DlqCount { get; private set; }

    public void Record(double publish, double deliver, double ack,
        long queueDepth, double mongoRate, double bodyRate, long bodyLag, double ttlDel, double dlqRate,
        double ping, double dataMb, double storageMb, double indexMb, double queueGrowthRate, string status)
    {
        Count++;
        Publish.Add(publish);
        Deliver.Add(deliver);
        Ack.Add(ack);
        QueueDepth.Add(queueDepth);
        MongoRate.Add(mongoRate);
        BodyRate.Add(bodyRate);
        BodyLag.Add(bodyLag);
        TtlDel.Add(ttlDel);
        DlqRate.Add(dlqRate);
        Ping.Add(ping);
        DataMb.Add(dataMb);
        StorageMb.Add(storageMb);
        IndexMb.Add(indexMb);
        QueueGrowthRate.Add(queueGrowthRate);

        if (status == "SLOW!") SlowCount++;
        if (status == "ERR") ErrorCount++;
        if (status == "BACKLOG") BacklogCount++;
        if (status == "DLQ!") DlqCount++;
    }
}
