using System.ComponentModel.DataAnnotations;

namespace OrderFlow.Domain;

public class ProcessedEvents
{
    [Key]
    public Guid EventId { get; set; }

    public string EventName { get; set; } = string.Empty;
}