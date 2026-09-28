using Confluent.SchemaRegistry;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using OrderFlow.Domain;
using OrderFlow.OrderStatus;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddHostedService<OrderStatusService>();

builder.Services.AddDbContext<OrdersProjectionDbContext>(opt =>
{
    opt.UseSqlite("Data Source=ordersprojection.db");
});

builder.Services.AddSingleton<ISchemaRegistryClient>(sp =>
{
    var configuration = sp.GetRequiredService<IConfiguration>();
    return new CachedSchemaRegistryClient(new SchemaRegistryConfig() { Url = configuration["SchemaRegistry:Url"] });
});

var host = builder.Build();
using (var scope = host.Services.CreateScope())
{
    scope.ServiceProvider.GetRequiredService<OrdersProjectionDbContext>().Database.EnsureCreated();
}
host.Run();
