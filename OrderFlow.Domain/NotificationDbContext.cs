using Microsoft.EntityFrameworkCore;

namespace OrderFlow.Domain;

public class NotificationDbContext : DbContext
{
    public NotificationDbContext(DbContextOptions<NotificationDbContext> options) : base(options){}
    
    public DbSet<Notification> Notifications { get; set; }
    
    protected override void OnModelCreating(ModelBuilder builder)
    {
        base.OnModelCreating(builder);
        builder.Entity<Notification>(b =>
        {
            b.HasIndex(n => new {n.OrderId, n.EventName}).IsUnique();
        });
    }
}