using Microsoft.EntityFrameworkCore;

namespace OrderFlow.Domain;

public class InventoryDbContext : DbContext
{
    public InventoryDbContext(DbContextOptions<InventoryDbContext> options) : base(options){}
    public DbSet<ProcessedEvents>  ProcessedEvents { get; set; }
    public DbSet<StockReservation> StockReservation { get; set; }

    protected override void OnModelCreating(ModelBuilder builder)
    {
        base.OnModelCreating(builder);
        builder.Entity<StockReservation>(b =>
        {
            b.HasIndex(k => k.OrderId).IsUnique();
        });
    }
}