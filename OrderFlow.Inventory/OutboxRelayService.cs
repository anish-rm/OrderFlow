using Confluent.Kafka;
using OrderFlow.Common.Constants;
using OrderFlow.Contracts;
using OrderFlow.Domain;
using System.Text.Json;

namespace OrderFlow.Inventory;

public class OutboxRelayService(IProducer<string, StockReserved> producer, 
    IServiceScopeFactory scopeFactory,
    ILogger<OutboxRelayService> logger) : BackgroundService
{
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    => Task.Run(async () =>
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var scope = scopeFactory.CreateScope();
                var dbContext = scope.ServiceProvider.GetRequiredService<InventoryDbContext>();

                var newEvent = dbContext.OutboxMessage
                    .Where(msg => msg.PublishedAt == null)
                    .OrderBy(msg => msg.Id)
                    .FirstOrDefault();

                if(newEvent == null)
                {
                    await Task.Delay(1000, stoppingToken);
                    continue;
                }

                try
                {
                    var stockReservation = JsonSerializer.Deserialize<StockReserved>(newEvent.Payload);
                    var message = new Message<string, StockReserved> { Key = newEvent.Key, Value = stockReservation! };
                    logger.LogInformation($"Producing message to Topic : " +
                        $"{TopicName.ORDEREVENTS} " +
                        $"Key :{message.Key} " +
                        $"CorrelationId: {stockReservation!.CorrelationId} " +
                        $"EventId : {stockReservation.EventId}");

                    var result = await producer.ProduceAsync(TopicName.ORDEREVENTS, message);
                        
                    logger.LogInformation($"Produced message to Topic :{result.Topic} Partition : {result.Partition.Value} Offset: {result.Offset.Value}");
                        
                    logger.LogInformation("Updating PublishedAt...");
                        
                    newEvent.PublishedAt = DateTime.UtcNow;
                    dbContext.SaveChanges();

                    logger.LogInformation("Updated PublishedAt");
                    await Task.Delay(1000);
                }
                catch(ProduceException<string, StockReserved> ex)
                {
                    logger.LogError($"Exception. Error Code: {ex.Error.Code} Status: {ex.DeliveryResult.Status} Ex: {ex}");
                    await Task.Delay(1000);
                }
                catch(OperationCanceledException ex)
                {
                    logger.LogInformation("Operation Cancelled");
                    break;
                }
                catch(Exception ex)
                {
                    logger.LogError($"Exception : {ex}");
                }
            }
            catch (Exception ex)
            {
                logger.LogError($"Exception : {ex}");
                await Task.Delay(1000);
            }
        }

    });
}
