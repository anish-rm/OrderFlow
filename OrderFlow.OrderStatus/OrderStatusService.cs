using Avro.Generic;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using OrderFlow.Common;
using OrderFlow.Common.Constants;
using OrderFlow.Common.Enums;
using OrderFlow.Contracts;
using OrderFlow.Domain;
using System.Text;
using OD = OrderFlow.Domain;

namespace OrderFlow.OrderStatus;

public class OrderStatusService(IServiceScopeFactory scopeFactory,
    IProducer<byte[], byte[]> dlqProducer,
    AvroSerializer<GenericRecord> avroSerializer,
    ILogger<OrderStatusService> logger) : BackgroundService
{
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    => Task.Run(async () =>
    {
        try
        {
            using var scope = scopeFactory.CreateScope();
            var configuration = scope.ServiceProvider.GetRequiredService<IConfiguration>();
            var config = new ConsumerConfig
            {
                BootstrapServers = configuration["Kafka:BootstrapServers"],
                GroupId = configuration["Kafka:GroupId"],
                EnableAutoCommit = true,
                EnableAutoOffsetStore = false,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
            };
            var srclient = scope.ServiceProvider.GetRequiredService<ISchemaRegistryClient>();

            var consumer = new ConsumerBuilder<string, GenericRecord>(config)
                            .SetValueDeserializer(new AvroDeserializer<GenericRecord>(srclient).AsSyncOverAsync())
                            .Build();

            consumer.Subscribe(TopicName.ORDEREVENTS);
            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    try
                    {
                        var result = consumer.Consume(stoppingToken);
                        var record = result.Message.Value;
                        try
                        {
                            switch (record.Schema.Name)
                            {
                                case EventTypes.ORDERPLACED:
                                    {
                                        var evt = MapToOrderPlaced(record);
                                        logger.LogInformation($"Received Event : {evt.EventName} Event Id : {evt.EventId} CorrelationId: {evt.CorrelationId} CausationId: {evt.CausationId}");

                                        using var innerscope = scopeFactory.CreateScope();
                                        using var dbcontext = innerscope.ServiceProvider.GetRequiredService<OrdersProjectionDbContext>();
                                        var incoming = (int)GetStatus(evt.EventName);
                                        var currentMax = await dbcontext.OrderStatuses.Where(os => os.OrderId == evt.OrderId).Select(os => (int?)os.Status).MaxAsync() ?? 0;
                                        if (incoming <= currentMax)
                                        {
                                            logger.LogInformation("Skipping...");
                                            consumer.StoreOffset(result);
                                            await Task.Delay(1000);
                                            continue;
                                        }
                                        await Retry.ExecuteWithRetry(async () =>
                                        {
                                            using var retryScope = scopeFactory.CreateScope();
                                            var retrydbcontext = retryScope.ServiceProvider.GetRequiredService<OD.OrdersProjectionDbContext>();
                                            logger.LogInformation($"Updating Status for OrderId : {evt.OrderId} status: {evt.EventName}");
                                            using var transaction = retrydbcontext.Database.BeginTransaction();

                                            var status = GetStatus(evt.EventName);
                                            var orderStatus = new OD.OrderStatus
                                            {
                                                CustomerId = evt.CustomerId,
                                                Status = status,
                                                OrderId = evt.OrderId,
                                                OccuredAt = evt.OccurredAt
                                            };
                                            retrydbcontext.OrderStatuses.Add(orderStatus);
                                            retrydbcontext.SaveChanges();
                                            transaction.Commit();
                                        }, isTransient);

                                        consumer.StoreOffset(result);
                                        break;
                                    }
                                case EventTypes.PAYMENTRECEIVED:
                                    {
                                        var evt = MapToPaymentReceived(record);
                                        logger.LogInformation($"Received Event : {evt.EventName} Event Id : {evt.EventId} CorrelationId: {evt.CorrelationId} CausationId: {evt.CausationId}");

                                        using var innerscope = scopeFactory.CreateScope();
                                        using var dbcontext = innerscope.ServiceProvider.GetRequiredService<OrdersProjectionDbContext>();
                                        var incoming = (int)GetStatus(evt.EventName);
                                        var currentMax = await dbcontext.OrderStatuses.Where(os => os.OrderId == evt.OrderId).Select(os => (int?)os.Status).MaxAsync() ?? 0;
                                        if (incoming <= currentMax)
                                        {
                                            logger.LogInformation("Skipping...");
                                            await Task.Delay(1000);
                                            continue;
                                        }
                                        await Retry.ExecuteWithRetry(async () =>
                                        {
                                            using var retryScope = scopeFactory.CreateScope();
                                            var retrydbcontext = retryScope.ServiceProvider.GetRequiredService<OD.OrdersProjectionDbContext>();
                                            logger.LogInformation($"Updating Status for OrderId : {evt.OrderId} status: {evt.EventName}");
                                            using var transaction = retrydbcontext.Database.BeginTransaction();

                                            var status = GetStatus(evt.EventName);

                                            var orderStatusData = retrydbcontext.OrderStatuses
                                                                .Where(os => os.OrderId == evt.OrderId)
                                                                .FirstOrDefault();
                                            if (orderStatusData == null)
                                            {
                                                var orderStatus = new OD.OrderStatus
                                                {
                                                    CustomerId = evt.CustomerId,
                                                    Status = status,
                                                    OrderId = evt.OrderId,
                                                    OccuredAt = evt.OccurredAt
                                                };
                                                retrydbcontext.OrderStatuses.Add(orderStatus);
                                            }
                                            else
                                            {
                                                orderStatusData.Status = status;
                                                orderStatusData.OccuredAt = evt.OccurredAt;
                                            }
                                            retrydbcontext.SaveChanges();
                                            transaction.Commit();
                                        }, isTransient);

                                        consumer.StoreOffset(result);
                                        break;
                                    }
                                case EventTypes.STOCKRESERVED:
                                    {
                                        var evt = MapToStockReserved(record);
                                        logger.LogInformation($"Received Event : {evt.EventName} Event Id : {evt.EventId} CorrelationId: {evt.CorrelationId} CausationId: {evt.CausationId}");

                                        using var innerscope = scopeFactory.CreateScope();
                                        using var dbcontext = innerscope.ServiceProvider.GetRequiredService<OrdersProjectionDbContext>();
                                        var incoming = (int)GetStatus(evt.EventName);
                                        var currentMax = await dbcontext.OrderStatuses.Where(os => os.OrderId == evt.OrderId).Select(os => (int?)os.Status).MaxAsync() ?? 0;
                                        if (incoming <= currentMax)
                                        {
                                            logger.LogInformation("Skipping...");
                                            await Task.Delay(1000);
                                            continue;
                                        }
                                        await Retry.ExecuteWithRetry(async () =>
                                        {
                                            using var retryScope = scopeFactory.CreateScope();
                                            var retrydbcontext = retryScope.ServiceProvider.GetRequiredService<OD.OrdersProjectionDbContext>();
                                            logger.LogInformation($"Updating Status for OrderId : {evt.OrderId} status: {evt.EventName}");
                                            using var transaction = retrydbcontext.Database.BeginTransaction();

                                            var status = GetStatus(evt.EventName);

                                            var orderStatusData = retrydbcontext.OrderStatuses
                                                                .Where(os => os.OrderId == evt.OrderId)
                                                                .FirstOrDefault();

                                            if (orderStatusData == null)
                                            {
                                                var orderStatus = new OD.OrderStatus
                                                {
                                                    CustomerId = evt.CustomerId,
                                                    Status = status,
                                                    OrderId = evt.OrderId,
                                                    OccuredAt = evt.OccurredAt
                                                };
                                                retrydbcontext.OrderStatuses.Add(orderStatus);
                                            }
                                            else
                                            {
                                                orderStatusData.Status = status;
                                                orderStatusData.OccuredAt = evt.OccurredAt;
                                            }
                                            retrydbcontext.SaveChanges();
                                            transaction.Commit();
                                        }, isTransient);

                                        consumer.StoreOffset(result);
                                        break;
                                    }
                                default:
                                    consumer.StoreOffset(result);
                                    break;
                            }
                        }
                        catch (RetryExhaustedException ex)
                        {
                            logger.LogError($"Exception : {ex.Message}");
                            var headers = new Headers
                            {
                                {"dlq.Exception", Encoding.UTF8.GetBytes(ex.ToString()) },
                                {"dlq.Topic", Encoding.UTF8.GetBytes(result.Topic) },
                                {"dlq.Partition", Encoding.UTF8.GetBytes(result.Partition.Value.ToString()) },
                                {"dlq.Offset", Encoding.UTF8.GetBytes(result.Offset.Value.ToString()) },
                                {"dlq.RetryAttempts", Encoding.UTF8.GetBytes(ex.Attempts.ToString()) },
                                {"dlq.ConsumerGroup", Encoding.UTF8.GetBytes(configuration["Kafka:GroupId"]) },
                                { "dlq.original-time", Encoding.UTF8.GetBytes(DateTime.UtcNow.ToString()) },
                            };

                            var message = new Message<byte[], byte[]>
                            {
                                Key = Encoding.UTF8.GetBytes(result.Message.Key),
                                Value = await avroSerializer.SerializeAsync(result.Message.Value, new SerializationContext(MessageComponentType.Value, result.Topic)),
                                Headers = headers
                            };

                            logger.LogInformation($"Pushing to DLQ: {TopicName.ORDEREVENTSDLQ}");
                            var dlqresult = await dlqProducer.ProduceAsync(TopicName.ORDEREVENTSDLQ, message);
                            logger.LogInformation(
                                    $"Produced event to DLQ. Partition : {dlqresult.Partition.Value} Offset : {dlqresult.Offset.Value}");
                            consumer.StoreOffset(result);
                        }
                        catch (Exception ex)
                        {
                            var headers = new Headers
                            {
                                {"dlq.Exception", Encoding.UTF8.GetBytes(ex.ToString()) },
                                {"dlq.Topic", Encoding.UTF8.GetBytes(result.Topic) },
                                {"dlq.Partition", Encoding.UTF8.GetBytes(result.Partition.Value.ToString()) },
                                {"dlq.Offset", Encoding.UTF8.GetBytes(result.Offset.Value.ToString()) },
                                {"dlq.RetryAttempts", Encoding.UTF8.GetBytes("0") },
                                {"dlq.ConsumerGroup", Encoding.UTF8.GetBytes(configuration["Kafka:GroupId"]) },
                                { "dlq.original-time", Encoding.UTF8.GetBytes(DateTime.UtcNow.ToString()) },
                            };

                            var message = new Message<byte[], byte[]>
                            {
                                Key = Encoding.UTF8.GetBytes(result.Message.Key),
                                Value = await avroSerializer.SerializeAsync(result.Message.Value, new SerializationContext(MessageComponentType.Value, result.Topic)),
                                Headers = headers
                            };

                            logger.LogInformation($"Pushing to DLQ: {TopicName.ORDEREVENTSDLQ}");
                            var dlqresult = await dlqProducer.ProduceAsync(TopicName.ORDEREVENTSDLQ, message);
                            logger.LogInformation(
                                    $"Produced event to DLQ. Partition : {dlqresult.Partition.Value} Offset : {dlqresult.Offset.Value}");
                            consumer.StoreOffset(result);
                        }
                    }
                    catch (ConsumeException ex)
                    {
                        var record = ex.ConsumerRecord;
                        var headers = new Headers
                        {
                            {"dlq.Exception", Encoding.UTF8.GetBytes(ex.ToString()) },
                            {"dlq.Topic", Encoding.UTF8.GetBytes(record.Topic) },
                            {"dlq.Partition", Encoding.UTF8.GetBytes(record.Partition.Value.ToString()) },
                            {"dlq.Offset", Encoding.UTF8.GetBytes(record.Offset.Value.ToString()) },
                            {"dlq.RetryAttempts", Encoding.UTF8.GetBytes("0") },
                            {"dlq.ConsumerGroup", Encoding.UTF8.GetBytes(configuration["Kafka:GroupId"]) },
                            { "dlq.original-time", Encoding.UTF8.GetBytes(DateTime.UtcNow.ToString()) },
                        };

                        var message = new Message<byte[], byte[]>
                        {
                            Key = record.Message.Key,
                            Value = record.Message.Value,
                            Headers = headers
                        };

                        logger.LogInformation($"Pushing to DLQ: {TopicName.ORDEREVENTSDLQ}");
                        var dlqresult = await dlqProducer.ProduceAsync(TopicName.ORDEREVENTSDLQ, message);
                        logger.LogInformation(
                                $"Produced event to DLQ. Partition : {dlqresult.Partition.Value} Offset : {dlqresult.Offset.Value}");
                        consumer.StoreOffset(new TopicPartitionOffset(record.TopicPartition, record.Offset + 1));
                    }

                }
            }
            catch (OperationCanceledException ex)
            {
                logger.LogError("Closing OrderStatus Consumer Service..");
            }
            finally
            {
                consumer.Close();
            }
        }
        catch (Exception ex)
        {
            logger.LogError($"Exception at startup of OrderStatusService : {ex}");
        }

    });
    private static PaymentReceived MapToPaymentReceived(GenericRecord r) => new PaymentReceived()
    {
        EventId = (Guid)r["EventId"],
        EventName = (string)r["EventName"],
        CustomerId = (string)r["CustomerId"],
        Amount = (double)r["Amount"],
        OrderId = (string)r["OrderId"],
        OccurredAt = (DateTime)r["OccurredAt"],
        EventVersion = (int)r["EventVersion"],
        CorrelationId = (string)r["CorrelationId"],
        CausationId = (string)r["CausationId"]
    };

    private OrderPlaced MapToOrderPlaced(GenericRecord record) => new OrderPlaced()
    {
        EventId = (Guid)record["EventId"],
        CustomerId = (string)record["CustomerId"],
        OrderId = (string)record["OrderId"],
        Amount = (double)record["Amount"],
        EventName = (string)record["EventName"],
        EventVersion = (int)record["EventVersion"],
        OccurredAt = (DateTime)record["OccurredAt"],
        CorrelationId = (string)record["CorrelationId"],
        CausationId = (string)record["CausationId"]
    };

    private StockReserved MapToStockReserved(GenericRecord record) => new StockReserved()
    {
        EventId = (Guid)record["EventId"],
        ReservationId = (Guid)record["ReservationId"],
        ReservationStatus = (string)record["ReservationStatus"],
        CustomerId = (string)record["CustomerId"],
        OrderId = (string)record["OrderId"],
        EventName = (string)record["EventName"],
        EventVersion = (int)record["EventVersion"],
        OccurredAt = (DateTime)record["OccurredAt"],
        CorrelationId = (string)record["CorrelationId"],
        CausationId = (string)record["CausationId"]
    };

    private static bool isTransient(Exception ex)
        => ex is HttpRequestException or TimeoutException
        || (ex is DbUpdateException { InnerException: SqliteException s } && s.SqliteErrorCode is 5 or 6);

    private OrderStatusEnum GetStatus(string status)
    {
        switch (status)
        {
            case EventTypes.ORDERPLACED:
                return OrderStatusEnum.OrderPlaced;
            case EventTypes.PAYMENTRECEIVED: 
                return OrderStatusEnum.PaymentReceived;
            case EventTypes.STOCKRESERVED:
                return OrderStatusEnum.StockReserved;
            default:
                return OrderStatusEnum.NA;
        }
    }
}
