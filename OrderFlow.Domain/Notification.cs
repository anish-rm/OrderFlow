namespace OrderFlow.Domain;

public class Notification 
{
    public int Id { get; set; }
    public required string OrderId { get; set; }
    public required string EventName { get; set; }
    public DateTime SentAt { get; set; } = DateTime.UtcNow;
}