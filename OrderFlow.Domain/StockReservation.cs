using System.ComponentModel.DataAnnotations;

namespace OrderFlow.Domain;

public class StockReservation
{
    [Key]
    public Guid ReservationId { get; set; }
    public required string OrderId { get; set; }
    public required string CustomerId { get; set; }
    public double Amount { get; set; }
    public DateTime ReservedAt { get; set; }
}