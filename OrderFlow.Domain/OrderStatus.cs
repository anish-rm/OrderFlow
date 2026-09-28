using OrderFlow.Common;
using osenum = OrderFlow.Common.Enums;


namespace OrderFlow.Domain;

public class OrderStatus
{
    public int Id { get; set; }
    public required string OrderId { get; set; }
    public required osenum.OrderStatus Status { get; set; }
    public required string CustomerId { get; set; }
    public DateTime OccuredAt { get; set; }
}
