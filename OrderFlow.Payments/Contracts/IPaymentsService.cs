using Confluent.Kafka;
using OrderFlow.Common;

namespace OrderFlow.Payments.Services;

public interface IPaymentsService
{
    Task<Result> PaymentFailedDLQPush(Message<byte[], byte[]> message);
}