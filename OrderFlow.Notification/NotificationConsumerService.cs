using System.Text;
using System.Text.Json;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using OrderFlow.Common;
using OrderFlow.Common.Constants;
using OrderFlow.Contracts;
using OrderFlow.Domain;

namespace OrderFlow.Notification;

public class NotificationConsumerService(IProducer<byte[], byte[]> producer,
    IServiceScopeFactory scopeFactory, 
    IConfiguration configuration,
    ILogger<NotificationConsumerService> logger) : BackgroundService
{
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
        => Task.Run(async () =>
        {
            var config = new ConsumerConfig
            {
                BootstrapServers =configuration["Kafka:BootstrapServers"], 
                GroupId =configuration["Kafka:GroupId"], 
                AutoOffsetReset = AutoOffsetReset.Latest,
                EnableAutoCommit = true,
                EnableAutoOffsetStore = false,
                PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
            };
            using var scope = scopeFactory.CreateScope();
            var srClient = scope.ServiceProvider.GetRequiredService<ISchemaRegistryClient>();
            using var consumer = new ConsumerBuilder<string, GenericRecord>(config)
                .SetValueDeserializer(new AvroDeserializer<GenericRecord>(srClient).AsSyncOverAsync())
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
                        logger.LogInformation($"Received message schema : {record.Schema.Name}");
                        try
                        {
                            switch (record.Schema.Name)
                            {
                                case EventTypes.ORDERPLACED:
                                {
                                    var orderPlacedEvent = MapToOrderPlaced(record);
                                    using var rscope = scopeFactory.CreateScope();
                                    var db = rscope.ServiceProvider.GetRequiredService<NotificationDbContext>();

                                    var orderExists = db.Notifications.Any(n =>
                                            n.OrderId == orderPlacedEvent.OrderId &&
                                            n.EventName == orderPlacedEvent.EventName);
                                    logger.LogInformation(
                                        $"STARTED OFFSET : {result.Offset} EventUd: {orderPlacedEvent.EventId} CorrelationId: {orderPlacedEvent.CorrelationId}");

                                    if (orderExists)
                                    {
                                        logger.LogInformation($"DUPLICATE EVENT. SKIPPING CorrelationId: {orderPlacedEvent.CorrelationId}");
                                        consumer.StoreOffset(result);
                                        continue;
                                    }
                                   
                                    await Retry.ExecuteWithRetry(async () =>
                                    {
                                        using var dbscope = scopeFactory.CreateScope();
                                        var notificationDbContext = dbscope.ServiceProvider.GetRequiredService<NotificationDbContext>();
                                        var emailSender = rscope.ServiceProvider.GetRequiredService<IEmailSender>();
                                        await emailSender.SendEmail(orderPlacedEvent.OrderId,
                                            orderPlacedEvent.EventName);
                                        logger.LogInformation($"EMAIL SENT CorrelationId: {orderPlacedEvent.CorrelationId}");
                                        using var transaction = notificationDbContext.Database.BeginTransaction();
                                        var notificationEvent = new Domain.Notification()
                                        {
                                            OrderId = orderPlacedEvent.OrderId,
                                            EventName = orderPlacedEvent.EventName,
                                        };
                                        notificationDbContext.Notifications.Add(notificationEvent);
                                        notificationDbContext.SaveChanges();
                                        transaction.Commit();
                                    }, isTransient);
                                    consumer.StoreOffset(result);
                                    break;
                                }
                                case EventTypes.PAYMENTRECEIVED:
                                {
                                    var paymentReceivedEvent = MapToPaymentReceived(record);
                                    using var rscope = scopeFactory.CreateScope();
                                    var db = rscope.ServiceProvider.GetRequiredService<NotificationDbContext>();

                                    var orderExists = db.Notifications.Any(n =>
                                        n.OrderId == paymentReceivedEvent.OrderId &&
                                        n.EventName == paymentReceivedEvent.EventName);
                                    
                                    logger.LogInformation(
                                        $"STARTED OFFSET : {result.Offset} EventUd: {paymentReceivedEvent.EventId} CorrelationId: {paymentReceivedEvent.CorrelationId}");

                                    if (orderExists)
                                    {
                                        logger.LogInformation($"DUPLICATE EVENT. SKIPPING CorrelationId: {paymentReceivedEvent.CorrelationId}");
                                        consumer.StoreOffset(result);
                                        continue;
                                    }
                                    await Retry.ExecuteWithRetry(async () =>
                                    {
                                        using var dbscope = scopeFactory.CreateScope();
                                        var notificationDbContext = dbscope.ServiceProvider.GetRequiredService<NotificationDbContext>();
                                        var emailSender = rscope.ServiceProvider.GetRequiredService<IEmailSender>();
                                        await emailSender.SendEmail(paymentReceivedEvent.OrderId,
                                            paymentReceivedEvent.EventName);
                                        
                                        using var transaction = notificationDbContext.Database.BeginTransaction();
                                        var notificationEvent = new Domain.Notification()
                                        {
                                            OrderId = paymentReceivedEvent.OrderId,
                                            EventName = paymentReceivedEvent.EventName,
                                        };
                                        notificationDbContext.Notifications.Add(notificationEvent);
                                        notificationDbContext.SaveChanges();
                                        transaction.Commit();
                                        logger.LogInformation($"EMAIL SENT for offset:{result.Offset} CorrelationId: {paymentReceivedEvent.CorrelationId}");
                                    }, isTransient);
                                    consumer.StoreOffset(result); 
                                    break;
                                }
                                default:
                                    logger.LogWarning($"Unknown Schema..");
                                    consumer.StoreOffset(result);
                                    break;
                            }
                        }
                        catch (RetryExhaustedException ex)
                        {
                            logger.LogError($"Retry Exhausted Exception Attempts: {ex.Attempts}");
                            var headers = new Headers()
                            {
                                { "dlq.exception", Encoding.UTF8.GetBytes(ex.ToString()) },
                                { "dlq.original-offset", Encoding.UTF8.GetBytes(result.Offset.Value.ToString()) },
                                { "dlq.original-topic", Encoding.UTF8.GetBytes(result.Topic) },
                                { "dlq.original-partition", Encoding.UTF8.GetBytes(result.Partition.Value.ToString()) },
                                { "dlq.original-time", Encoding.UTF8.GetBytes(DateTime.UtcNow.ToString()) },
                                { "dlq.consumer-group", Encoding.UTF8.GetBytes(configuration["Kafka:GroupId"])},
                                { "redrive.attempt", Encoding.UTF8.GetBytes(ex.Attempts.ToString()) },
                            };
                            var message = new Message<byte[], byte[]>()
                            {
                                Key = Encoding.UTF8.GetBytes(result.Message.Key),
                                Value = JsonSerializer.SerializeToUtf8Bytes(result.Message.Value),
                                Headers = headers
                            };
                            logger.LogInformation($"Pushing to DLQ: {TopicName.ORDEREVENTSDLQ}");
                            var dlqresult = await producer.ProduceAsync(TopicName.ORDEREVENTSDLQ, message);
                            logger.LogInformation(
                                $"Produced event to DLQ. Partition : {dlqresult.Partition.Value} Offset : {dlqresult.Offset.Value}");
                            consumer.StoreOffset(result);

                        }
                        catch (Exception ex)
                        {
                            if (ex is DbUpdateException { InnerException: SqliteException { SqliteErrorCode: 19 } })
                            {
                                logger.LogInformation("DUPLICATE EVENT. SKIPPING..");
                                consumer.StoreOffset(result);
                            }
                            else
                            {
                               result.Message.Headers.TryGetLastBytes("redrive.attempt", out var bytes);
                                var retryCountNo = 0;
                                if (bytes != null)
                                {
                                    var retryCount = Encoding.UTF8.GetString(bytes);
                                    retryCountNo = int.Parse(retryCount) + 1;
                                }
                                var headers = new Headers()
                                {
                                    { "dlq.exception", Encoding.UTF8.GetBytes(ex.ToString()) },
                                    { "dlq.original-offset", Encoding.UTF8.GetBytes(result.Offset.Value.ToString()) },
                                    { "dlq.original-topic", Encoding.UTF8.GetBytes(result.Topic) },
                                    { "dlq.original-partition", Encoding.UTF8.GetBytes(result.Partition.Value.ToString()) },
                                    { "dlq.original-time", Encoding.UTF8.GetBytes(DateTime.UtcNow.ToString()) },
                                    { "dlq.consumer-group", Encoding.UTF8.GetBytes(configuration["Kafka:GroupId"])},
                                    { "redrive.attempt", Encoding.UTF8.GetBytes(retryCountNo.ToString())},
                                };
                                var message = new Message<byte[], byte[]>()
                                {
                                    Key = Encoding.UTF8.GetBytes(result.Message.Key),
                                    Value = JsonSerializer.SerializeToUtf8Bytes(result.Message.Value),
                                    Headers = headers
                                };
                                logger.LogInformation($"Pushing to DLQ: {TopicName.ORDEREVENTSDLQ}");
                                var dlqresult = await producer.ProduceAsync(TopicName.ORDEREVENTSDLQ, message);
                                logger.LogInformation(
                                    $"Produced event to DLQ. Partition : {dlqresult.Partition.Value} Offset : {dlqresult.Offset.Value}");
                                consumer.StoreOffset(result);
                            }
                        }
                    }
                    catch (ConsumeException ex)
                    {
                        logger.LogError($"Consume Error Code : {ex.Error.Code}  Reason: {ex.Error.Reason} Message : {ex.Message}");
                        var result = ex.ConsumerRecord;
                        ex.ConsumerRecord.Message.Headers.TryGetLastBytes("redrive.attempt", out var bytes);
                        var retryCountNo = 0;
                        if (bytes != null)
                        {
                            var retryCount = System.Text.Encoding.UTF8.GetString(bytes);
                            retryCountNo = int.Parse(retryCount) + 1;
                        }
                        var headers = new Headers()
                        {
                            { "dlq.exception", Encoding.UTF8.GetBytes(ex.ToString()) },
                            { "dlq.original-offset", Encoding.UTF8.GetBytes(result.Offset.Value.ToString()) },
                            { "dlq.original-topic", Encoding.UTF8.GetBytes(result.Topic) },
                            { "dlq.original-partition", Encoding.UTF8.GetBytes(result.Partition.Value.ToString()) },
                            { "dlq.original-time", Encoding.UTF8.GetBytes(DateTime.UtcNow.ToString()) },
                            { "dlq.consumer-group", Encoding.UTF8.GetBytes(configuration["Kafka:GroupId"])},
                            { "redrive.attempt", Encoding.UTF8.GetBytes(retryCountNo.ToString())},
                        };
                        var message = new Message<byte[], byte[]>()
                        {
                            Key = result.Message.Key, 
                            Value =result.Message.Value, 
                            Headers = headers
                        };
                        logger.LogInformation($"Pushing to DLQ: {TopicName.ORDEREVENTSDLQ}");
                        var dlqresult = await producer.ProduceAsync(TopicName.ORDEREVENTSDLQ, message);
                        logger.LogInformation(
                            $"Produced event to DLQ. Partition : {dlqresult.Partition.Value} Offset : {dlqresult.Offset.Value}");
                        consumer.StoreOffset(new TopicPartitionOffset(result.TopicPartition, result.Offset + 1));
                    }
                  
                }
            }
            catch (OperationCanceledException ex)
            {
                logger.LogError("Closing Notification Consumer Service..");
            }
            finally
            {
                consumer.Close();
            }
        });
    
    private static bool isTransient(Exception ex)
        => ex is HttpRequestException or TimeoutException
           || (ex is DbUpdateException {InnerException: SqliteException s} && s.SqliteErrorCode is 5 or 6);

    private OrderPlaced MapToOrderPlaced(GenericRecord record) => new OrderPlaced()
    {
        EventId = (Guid)record["EventId"],
        CustomerId = (string)record["CustomerId"],
        OrderId = (string)record["OrderId"],
        Amount = (double)record["Amount"],
        EventName = (string)record["EventName"],
        EventVersion = (int)record["EventVersion"],
        OccurredAt = (DateTime)record["OccurredAt"],
        CorrelationId = record.Schema.Fields.Any(f => f.Name == "CorrelationId")
            ? (string)record["CorrelationId"]
            : "",
        CausationId = record.Schema.Fields.Any(f => f.Name == "CausationId")
            ? (string)record["CausationId"]
            : ""
    };
    
    private PaymentReceived MapToPaymentReceived(GenericRecord record) => new PaymentReceived()
    {
        EventId = (Guid)record["EventId"],
        CustomerId = (string)record["CustomerId"],
        OrderId = (string)record["OrderId"],
        Amount = (double)record["Amount"],
        EventName = (string)record["EventName"],
        EventVersion = (int)record["EventVersion"],
        OccurredAt = (DateTime)record["OccurredAt"],
        CorrelationId = record.Schema.Fields.Any(f => f.Name == "CorrelationId")
            ? (string)record["CorrelationId"]
            : "",
        CausationId = record.Schema.Fields.Any(f => f.Name == "CausationId")
            ? (string)record["CausationId"]
            : ""
    };
}