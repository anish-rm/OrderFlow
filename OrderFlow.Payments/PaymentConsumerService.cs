using System.Text;
using System.Text.Json;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using OrderFlow.Application.Dtos;
using OrderFlow.Common;
using OrderFlow.Common.Constants;
using OrderFlow.Contracts;
using OrderFlow.Domain;
using OrderFlow.Payments.Services;

namespace OrderFlow.Payments;

public class PaymentConsumerService(IConfiguration configuration, ILogger<PaymentConsumerService> logger, IServiceScopeFactory scopeFactory) : BackgroundService
{
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
        => Task.Run(async () =>
        {
            logger.LogInformation("Starting PaymentConsumerService");
            var config = new ConsumerConfig
            {
                BootstrapServers = configuration["Kafka:BootstrapServers"],
                GroupId = configuration["Kafka:GroupId"],
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = true,
                EnableAutoOffsetStore = false,
                PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky
            };
            
            using var scope = scopeFactory.CreateScope();
            using var srclient = scope.ServiceProvider.GetRequiredService<ISchemaRegistryClient>();

            using var consumer = new ConsumerBuilder<string, GenericRecord>(config)
                .SetValueDeserializer(new AvroDeserializer<GenericRecord>(srclient).AsSyncOverAsync())
                .Build();
            
            consumer.Subscribe(TopicName.ORDEREVENTS);
            
            try
            {
                int cnt = 0;
                while (!stoppingToken.IsCancellationRequested)
                {
                    using var rscope =  scopeFactory.CreateScope();
                    try
                    {
                        var result = consumer.Consume(stoppingToken);

                        var record = result.Message.Value;
                        
                        logger.LogInformation($"Received Event {record.Schema.Name}");

                        switch (record.Schema.Name)
                        {
                            case EventTypes.ORDERPLACED: 
                            {
                                var evt = MapToPaymentReceived(record);
                                 logger.LogInformation(
                                    $"STARTED OFFSET : {result.Offset} EventUd: {evt.EventId} correlationId: {evt.CorrelationId} causationId: {evt.CausationId}");

                            var db = rscope.ServiceProvider.GetRequiredService<PaymentsDbContext>();
                            var eventExists = db.ProcessedEvents
                                .Find(evt.EventId);
                            if (eventExists != null)
                            {
                                logger.LogInformation("DUPLICATE EVENT. SKIPPING..");
                                consumer.StoreOffset(result);
                                continue;
                            }

                       
                            try
                            {
                                await Retry.ExecuteWithRetry(async () =>
                                {
                                    using var ascope =  scopeFactory.CreateScope();
                                    var sdb = ascope.ServiceProvider.GetRequiredService<PaymentsDbContext>();
                                    using var transaction = sdb.Database.BeginTransaction();
                                    // Thread.Sleep(3000);
                                    // if (result.Message.Key == "FAIL-TWICE")
                                    // {
                                    //     logger.LogError($"Failed {cnt}");
                                    //     if (cnt < 2)
                                    //     {
                                    //         cnt += 1;
                                    //         throw new HttpRequestException("Unforturnately service is down");
                                    //     }
                                    //     else if(cnt == 2)
                                    //     {
                                    //         cnt = 0;
                                    //     }
                                    // }
                                    var payment = new Domain.Payments()
                                    {
                                        CustomerId = evt.CustomerId,
                                        Amount = evt.Amount,
                                        OrderId = evt.OrderId,
                                        ChargedAt = DateTime.UtcNow,
                                    };
                                    var processedEvent = new ProcessedEvents()
                                    {
                                        EventId = evt.EventId,
                                        EventName = evt.EventName
                                    };
                                    var paymentEvent = new PaymentReceived()
                                    {
                                        EventId = Guid.NewGuid(),
                                        EventName = EventTypes.PAYMENTRECEIVED,
                                        OrderId = evt.OrderId,
                                        CustomerId = evt.CustomerId,
                                        Amount = evt.Amount,
                                        OccurredAt = DateTime.UtcNow,
                                        EventVersion = 1,
                                        CorrelationId = evt.CorrelationId,
                                        CausationId = evt.EventId.ToString()
                                    };
                                    var outboxMessage = new OutboxMessage()
                                    {
                                        EventId = paymentEvent.EventId,
                                        Topic = TopicName.ORDEREVENTS,
                                        Key = evt.OrderId,
                                        Payload = JsonSerializer.Serialize(paymentEvent),
                                    };
                                    sdb.Payments.Add(payment);
                                    sdb.ProcessedEvents.Add(processedEvent);
                                    sdb.OutboxMessages.Add(outboxMessage);
                                    sdb.SaveChanges();
                                    transaction.Commit();
                                    logger.LogInformation(
                                        $"CHARGED OFFSET : {result.Offset} EventId: {evt.EventId}");
                                }, isTransient);
                                consumer.StoreOffset(result);
                            }
                            catch (RetryExhaustedException ex)
                            {
                                var headers = new Headers
                                {
                                    { "dlq.exception", Encoding.UTF8.GetBytes(ex.ToString()) },
                                    { "dlq.original-offset", Encoding.UTF8.GetBytes(result.Offset.Value.ToString()) },
                                    { "dlq.original-topic", Encoding.UTF8.GetBytes(result.Topic) },
                                    { "dlq.original-partition", Encoding.UTF8.GetBytes(result.Partition.Value.ToString()) },
                                    { "dlq.original-time", Encoding.UTF8.GetBytes(DateTime.UtcNow.ToString()) },
                                    { "dlq.consumergroup", Encoding.UTF8.GetBytes(configuration["Kafka:GroupId"]!)},
                                    { "dlq.attempts", Encoding.UTF8.GetBytes(ex.Attempts.ToString())},
                                };
                                var message = new Message<byte[], byte[]>
                                {
                                    Key = Encoding.UTF8.GetBytes(result.Message.Key), 
                                    Value = JsonSerializer.SerializeToUtf8Bytes(result.Message.Value),
                                    Headers = headers
                                };
                                logger.LogError(ex, ex.Message);
                                var paymentService = scope.ServiceProvider.GetRequiredService<IPaymentsService>();
                                var paymentProduceResult =
                                    await paymentService.PaymentFailedDLQPush(message);
                                if (paymentProduceResult.IsSuccess)
                                {
                                    consumer.StoreOffset(result);
                                }
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
                                        var retryCount = System.Text.Encoding.UTF8.GetString(bytes);
                                        retryCountNo = int.Parse(retryCount) + 1;
                                    }
                                    var headers = new Headers
                                    {
                                        { "dlq.exception", Encoding.UTF8.GetBytes(ex.ToString()) },
                                        { "dlq.original-offset", Encoding.UTF8.GetBytes(result.Offset.Value.ToString()) },
                                        { "dlq.original-topic", Encoding.UTF8.GetBytes(result.Topic) },
                                        { "dlq.original-partition", Encoding.UTF8.GetBytes(result.Partition.Value.ToString()) },
                                        { "dlq.original-time", Encoding.UTF8.GetBytes(DateTime.UtcNow.ToString()) },
                                        { "dlq.consumergroup", Encoding.UTF8.GetBytes(configuration["Kafka:GroupId"]!)},
                                        { "dlq.attempts", Encoding.UTF8.GetBytes("0")},
                                        { "redrive.attempt", Encoding.UTF8.GetBytes(retryCountNo.ToString())},
                                    };
                                    var message = new Message<byte[], byte[]>
                                    {
                                        Key = Encoding.UTF8.GetBytes(result.Message.Key), 
                                        Value = JsonSerializer.SerializeToUtf8Bytes(result.Message.Value),
                                        Headers = headers
                                    };
                                    logger.LogError(ex, ex.Message);
                                    var paymentService = scope.ServiceProvider.GetRequiredService<IPaymentsService>();
                                    var paymentProduceResult =
                                        await paymentService.PaymentFailedDLQPush(message);
                                    if (paymentProduceResult.IsSuccess)
                                    {
                                        consumer.StoreOffset(result);
                                    }
                                }
                            }

                            break;
                            }
                            default:
                            {
                                consumer.StoreOffset(result);
                                break;
                            }
                        }

                       
                    }
                    catch (ConsumeException ex)
                    {
                        var result = ex.ConsumerRecord;
                        ex.ConsumerRecord.Message.Headers.TryGetLastBytes("redrive.attempt", out var bytes);
                        var retryCountNo = 0;
                        if (bytes != null)
                        {
                            var retryCount = System.Text.Encoding.UTF8.GetString(bytes);
                            retryCountNo = int.Parse(retryCount) + 1;
                        }
                        
                        var headers = new Headers
                        {
                            { "dlq.exception", Encoding.UTF8.GetBytes(ex.ToString()) },
                            { "dlq.original-offset", Encoding.UTF8.GetBytes(result.Offset.Value.ToString()) },
                            { "dlq.original-topic", Encoding.UTF8.GetBytes(result.Topic) },
                            { "dlq.original-partition", Encoding.UTF8.GetBytes(result.Partition.Value.ToString()) },
                            { "dlq.original-time", Encoding.UTF8.GetBytes(DateTime.UtcNow.ToString()) },
                            { "dlq.consumergroup", Encoding.UTF8.GetBytes(configuration["Kafka:GroupId"]!)},
                            { "redrive.attempt", Encoding.UTF8.GetBytes(retryCountNo.ToString())},
                        };
                        var message = new Message<byte[], byte[]> { Key = result.Message.Key, Value = result.Message.Value, Headers = headers };
                        logger.LogError(ex, ex.Message);
                        var paymentService = scope.ServiceProvider.GetRequiredService<IPaymentsService>();
                        var paymentProduceResult =
                            await paymentService.PaymentFailedDLQPush(message);
                        if (paymentProduceResult.IsSuccess)
                        {
                            consumer.StoreOffset(new TopicPartitionOffset(result.TopicPartition, result.Offset +1));
                        }
                    }
                }
            }
            catch (OperationCanceledException ex)
            {
                logger.LogInformation("Closing consumer...");
            }
            finally
            {
                consumer.Close();
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
        CorrelationId = r.Schema.Fields.Any(f => f.Name == "CorrelationId")
            ? (string)r["CorrelationId"]
            : "",
        CausationId = r.Schema.Fields.Any(f => f.Name == "CausationId") 
            ? (string)r["CausationId"]
            : ""
    };
    private static bool isTransient(Exception ex)
        => ex is HttpRequestException or TimeoutException
        || (ex is DbUpdateException {InnerException: SqliteException s} && s.SqliteErrorCode is 5 or 6);
}