using Avro;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using OrderFlow.Common.Constants;
using OrderFlow.Contracts;
using OrderFlow.Domain;
using System.Text.Json;

namespace OrderFlow.Inventory;

public class InventoryConsumerService(IConfiguration configuration, ILogger<InventoryConsumerService> logger, IServiceScopeFactory scopeFactory): BackgroundService
{
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
        => Task.Run(() =>
        {
            // var configuration = 
            logger.LogInformation("Starting InventoryConsumerService");
            var config = new ConsumerConfig()
            {
                BootstrapServers = configuration["Kafka:BootstrapServers"],
                GroupId = configuration["Kafka:GroupId"],
                EnableAutoCommit = true,
                EnableAutoOffsetStore = false,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky
            };

            using var scope = scopeFactory.CreateScope();

            using var srclient = scope.ServiceProvider.GetRequiredService<ISchemaRegistryClient>();
            using var consumer = new ConsumerBuilder<string, GenericRecord>(config)
                .SetValueDeserializer(new AvroDeserializer<GenericRecord>(srclient).AsSyncOverAsync())
                .SetPartitionsAssignedHandler((c, parts) =>  logger.LogInformation($"ASSIGNED: [{string.Join(",", parts)}]"))
                .SetPartitionsRevokedHandler((c, parts) =>  logger.LogInformation($"REVOKED:  [{string.Join(",", parts)}]"))
                .SetPartitionsLostHandler((c, parts)    =>  logger.LogInformation($"LOST:     [{string.Join(",", parts)}]"))
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
                        switch (record.Schema.Name)                          
                        {
                            case EventTypes.PAYMENTRECEIVED: 
                            {
                                var evt = MapToPaymentReceived(record);
                                logger.LogInformation(
                                    $"STARTED OFFSET : {result.Offset} EventUd: {evt.EventId} correlationId: {evt.CorrelationId} causationId: {evt.CausationId}");
                                using var rscope = scopeFactory.CreateScope();
                                var db = rscope.ServiceProvider.GetRequiredService<InventoryDbContext>();
                                var paymentEventExists = db.ProcessedEvents.Find(evt.EventId);
                                if (paymentEventExists != null)
                                {
                                    logger.LogInformation("DUPLICATE EVENT. SKIPPING..");
                                    consumer.StoreOffset(result);
                                    continue;
                                }

                                Thread.Sleep(1000);
                                using var transaction = db.Database.BeginTransaction();
                                try
                                {
                                    var reservation = new StockReservation()
                                    {
                                        ReservationId = Guid.NewGuid(),
                                        Amount = evt.Amount,
                                        CustomerId = evt.CustomerId,
                                        OrderId = evt.OrderId,
                                        ReservedAt = DateTime.UtcNow
                                    };

                                    var processedEvent = new ProcessedEvents()
                                    {
                                        EventId = evt.EventId,
                                        EventName = evt.EventName
                                    };

                                    var stockReservedPayload = new StockReserved()
                                    {
                                        EventId = Guid.NewGuid(),
                                        EventName = EventTypes.STOCKRESERVED,
                                        ReservationId = reservation.ReservationId,
                                        CustomerId = reservation.CustomerId,
                                        OrderId = reservation.OrderId,
                                        OccurredAt = DateTime.UtcNow,
                                        CorrelationId = evt.CorrelationId,
                                        CausationId = evt.CausationId,
                                        ReservationStatus = "Reserved",
                                        EventVersion = 1
                                    };

                                    var outboxMessage = new OutboxMessage()
                                    {
                                        EventId = stockReservedPayload.EventId,
                                        Topic = TopicName.ORDEREVENTS,
                                        Key = evt.OrderId,
                                        Payload = JsonSerializer.Serialize(stockReservedPayload),
                                    };

                                    db.StockReservation.Add(reservation);
                                    db.ProcessedEvents.Add(processedEvent);
                                    db.OutboxMessage.Add(outboxMessage);
                                    db.SaveChanges();
                                    transaction.Commit();
                                    logger.LogInformation(
                                        $"RESERVED OFFSET : {result.Offset} EventUd: {evt.EventId}");
                                    consumer.StoreOffset(result);
                                }
                                catch (DbUpdateException ex)
                                {
                                    if (ex.InnerException is SqliteException sqliteException &&
                                        sqliteException.SqliteErrorCode == 19)
                                    {
                                        logger.LogInformation("DUPLICATE EVENT. SKIPPING..");
                                        consumer.StoreOffset(result);
                                    }
                                    else
                                    {
                                        throw;
                                    }
                                }

                                break;
                            }
                            default:                                          // OrderPlaced, future OrderShipped, anything else
                                consumer.StoreOffset(result);                 // every-exit-stores, as always
                                break;
                        }
                    }
                    catch (ConsumeException ex)
                    {
                       logger.LogError(ex, "Consume error"); 
                    }
                }
            }
            catch (OperationCanceledException ex)
            {
                logger.LogInformation("Closing Comnsumer..");
            }
            finally
            {
                consumer.Close();
            }
        });
    
    private static PaymentReceived MapToPaymentReceived(GenericRecord r) => new PaymentReceived
    {
        EventId      = (Guid)r["EventId"],          
        EventName    = (string)r["EventName"],
        OrderId      = (string)r["OrderId"],
        CustomerId   = (string)r["CustomerId"],
        Amount       = (double)r["Amount"],
        OccurredAt   = ((DateTime)r["OccurredAt"]),               
        EventVersion = (int)r["EventVersion"],
        CorrelationId = r.Schema.Fields.Any(f => f.Name == "CorrelationId")
            ? (string)r["CorrelationId"]
            : "",
        CausationId = r.Schema.Fields.Any(f => f.Name == "CausationId") 
            ? (string)r["CausationId"]
            : ""
    };
}