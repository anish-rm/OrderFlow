using Microsoft.EntityFrameworkCore;

namespace OrderFlow.Domain;

public class OrdersProjectionDbContext : DbContext
{
    public OrdersProjectionDbContext(DbContextOptions<OrdersProjectionDbContext> options) : base(options)
    {
    }

    public DbSet<OrderStatus> OrderStatuses { get; set; }

    protected override void OnModelCreating(ModelBuilder builder)
    {
        base.OnModelCreating(builder);
        builder.Entity<OrderStatus>(b =>
        {
            b.HasIndex(k => k.OrderId).IsUnique();
        });
    }
}
