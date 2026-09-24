namespace OrderFlow.Application.Dtos;

public class PlaceOrderRequestDto
{
    public required string CustomerId { get; set; }
    public double Amount { get; set; }
}