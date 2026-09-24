namespace OrderFlow.Application.Dtos;

public class PaymentReceivedProduceDto
{
    public Guid EventId { get; set; }
    public required string CustomerId { get; set; }
    public required string OrderId { get; set; }
    public decimal Amount { get; set; }
}