using Microsoft.EntityFrameworkCore;

namespace OrderFlow.Domain;

public class PaymentsDbContext : DbContext 
{
    public PaymentsDbContext(DbContextOptions<PaymentsDbContext> options) : base(options){}
    public DbSet<ProcessedEvents>  ProcessedEvents { get; set; }
    public DbSet<Payments>  Payments { get; set; }
    
    public DbSet<OutboxMessage> OutboxMessages { get; set; }
    
}