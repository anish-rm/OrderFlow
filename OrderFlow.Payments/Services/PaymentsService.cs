using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using OrderFlow.Application.Dtos;
using OrderFlow.Common;
using OrderFlow.Common.Constants;
using OrderFlow.Contracts;
using Error = OrderFlow.Common.Error;

namespace OrderFlow.Payments.Services;

public class PaymentsService( ILogger<PaymentsService> logger, IProducer<byte[], byte[]> dlqProducer) : IPaymentsService
{
    public async Task<Result> PaymentFailedDLQPush(Message<byte[], byte[]> message)
    {
        try
        {
            var result = await dlqProducer.ProduceAsync(TopicName.ORDEREVENTSDLQ, message);
            logger.LogInformation(
                $"Produced event to DLQ. Partition : {result.Partition.Value} Offset : {result.Offset.Value}");
            return Result.Success();
        }
        catch (ProduceException<byte[], byte[]> ex)
        {
            logger.LogError($"Code : {ex.Error.Code}, Status: {ex.DeliveryResult.Status}, Message: {ex.Message}");
            return Result.Failure(new Error(ErrorCodes.Timeout, "Try again after some time"));
        }
    }
}