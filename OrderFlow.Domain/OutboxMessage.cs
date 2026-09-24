namespace OrderFlow.Domain;

public class OutboxMessage
{
    public int Id { get; set; }
    public Guid EventId { get; set; }
    public required string Topic { get; set; }
    public string Key { get; set; } = string.Empty;
    public string Payload { get; set; } = string.Empty;
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    public DateTime? PublishedAt { get; set; }
    
}