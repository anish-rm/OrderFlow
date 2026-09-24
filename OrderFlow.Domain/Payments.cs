namespace OrderFlow.Domain;

public class Payments
{
    public int Id { get; set; }
    public required string OrderId { get; set; }
    public required string CustomerId { get; set; }
    public double Amount { get; set; }
    public DateTime ChargedAt { get; set; }
}