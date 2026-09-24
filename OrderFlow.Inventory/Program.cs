using Confluent.SchemaRegistry;
using Microsoft.EntityFrameworkCore;
using OrderFlow.Application.Services;
using OrderFlow.Domain;
using OrderFlow.Inventory;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddHostedService<InventoryConsumerService>();
builder.Services.AddDbContext<InventoryDbContext>(options => {
    options.UseSqlite("Data Source=inventory.db");
});

builder.Services.AddSingleton<ISchemaRegistryClient>(sp =>
{
    var configuration = sp.GetRequiredService<IConfiguration>();
    return new CachedSchemaRegistryClient(new SchemaRegistryConfig()
    {
        Url = configuration["SchemaRegistry:Url"]
    });
});


var host = builder.Build();
using (var scope = host.Services.CreateScope())
{
    var services = scope.ServiceProvider.GetRequiredService<InventoryDbContext>().Database.EnsureCreated();
}
host.Run();