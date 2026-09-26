using System.Text.Json;
using Confluent.Kafka;
using OrderFlow.Common.Constants;
using OrderFlow.Contracts;
using OrderFlow.Domain;

namespace OrderFlow.Payments;

public class OutboxRelayService(IProducer<string, PaymentReceived> producer, 
    IServiceScopeFactory scopeFactory, 
    ILogger<OutboxRelayService> logger) : BackgroundService
{
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
        => Task.Run(async () =>
        {
            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    try
                    {
                        using var scope = scopeFactory.CreateScope();
                        var db = scope.ServiceProvider.GetRequiredService<PaymentsDbContext>();

                        var outboxMessage = db.OutboxMessages
                            .Where(om => om.PublishedAt == null)
                            .OrderBy(om => om.Id)
                            .FirstOrDefault();
                        if (outboxMessage == null)
                        {
                            await Task.Delay(1000, stoppingToken);
                            continue;
                        }

                        var PaymentReceivedEvent = JsonSerializer.Deserialize<PaymentReceived>(outboxMessage.Payload);
                        var message = new Message<string, PaymentReceived>()
                            { Key = outboxMessage.Key, Value = PaymentReceivedEvent};
                        var result = await producer.ProduceAsync(TopicName.ORDEREVENTS, message);
                        logger.LogInformation(
                            $"Published EventId : {outboxMessage.EventId} to Partition :{result.Partition.Value} Offset :{result.Offset.Value} correlationId: {PaymentReceivedEvent.CorrelationId}");
                        await Task.Delay(5000, stoppingToken);
                        outboxMessage.PublishedAt = DateTime.UtcNow;
                        db.SaveChanges();
                    }
                    catch (ProduceException<string, string> ex)
                    {
                        logger.LogError(
                            $"Code : {ex.Error.Code}, Status: {ex.DeliveryResult.Status}, Message: {ex.Message}");
                        await Task.Delay(1000, stoppingToken);
                    }
                    catch (OperationCanceledException ex)
                    {
                        logger.LogInformation("Closing Outbox service");
                        break;
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex, ex.Message);
                        await Task.Delay(1000, stoppingToken);
                    }
                }
            }
            catch (Exception ex)
            {
                logger.LogError(ex, ex.Message);
            } 
        });
}