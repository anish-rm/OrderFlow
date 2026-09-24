using Confluent.Kafka;
using OrderFlow.Application.Dtos;
using OrderFlow.Common;
using OrderFlow.Common.Constants;
using OrderFlow.Contracts;
using Error = OrderFlow.Common.Error;
using Microsoft.Extensions.Logging;
using Microsoft.AspNetCore.Http;

namespace OrderFlow.Application.Services;

public class OrdersService(IHttpContextAccessor httpContextAccessor, IProducer<string, OrderPlaced> producer, ILogger<OrdersService> logger) : IOrdersService
{
    public async Task<Result<OrderIdDto>> PlaceOrderRequestAsync(PlaceOrderRequestDto placeOrderRequestDto)
    {
        if (placeOrderRequestDto.Amount <= 0)
        {
           return Result<OrderIdDto>.BadRequest(new Error(ErrorCodes.BadRequest, "Amount must be greater than zero"));
        }
        
        if(string.IsNullOrEmpty(placeOrderRequestDto.CustomerId))
        {
            return Result<OrderIdDto>.BadRequest(new Error(ErrorCodes.BadRequest, "CustomerId must be provided"));
        }

        var correlationId = httpContextAccessor.HttpContext?.Request.Headers["CorrelationId"];
        if (string.IsNullOrEmpty(correlationId))
        {
            correlationId = Guid.NewGuid().ToString();
        }
        logger.LogInformation($"Starting Order Accepted : {correlationId}");
        var orderEvent = new OrderPlaced()
        {
            EventId = Guid.NewGuid(),
            EventName = EventTypes.ORDERPLACED,
            OrderId = Guid.NewGuid().ToString(),
            CustomerId = placeOrderRequestDto.CustomerId,
            Amount = placeOrderRequestDto.Amount,
            OccurredAt = DateTime.UtcNow,
            EventVersion = 1,
            CorrelationId = correlationId,
            CausationId = ""
        };

        try
        {
            logger.LogInformation($"Order Accepeted : {correlationId}, eventId: {orderEvent.EventId}, orderId: {orderEvent.OrderId}");
            var result = await producer.ProduceAsync(TopicName.ORDEREVENTS, new Message<string, OrderPlaced>
            {
                Key  = orderEvent.OrderId,
                Value = orderEvent
            });
            logger.LogInformation($"Produced order event Partition : {result.Partition.Value} Offset : {result.Offset.Value}");
            return Result<OrderIdDto>.Success(new OrderIdDto{OrderId = orderEvent.OrderId});
        }
        catch (ProduceException<string, OrderPlaced> ex)
        {
            logger.LogError($"Code : {ex.Error.Code}, Status: {ex.DeliveryResult.Status}, Message: {ex.Message} {ex}");
            return Result<OrderIdDto>.Failure(new Error(ErrorCodes.Timeout, "Try again after some time"));
        }
        
    }
}