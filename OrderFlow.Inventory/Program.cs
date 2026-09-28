using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.EntityFrameworkCore;
using OrderFlow.Contracts;
using OrderFlow.Domain;
using OrderFlow.Inventory;
using Serilog;

var builder = Host.CreateApplicationBuilder(args);

Log.Logger = new LoggerConfiguration()
    .ReadFrom.Configuration(builder.Configuration)
    .CreateLogger();
builder.Logging.ClearProviders();
builder.Services.AddSerilog();

builder.Services.AddHostedService<InventoryConsumerService>();
builder.Services.AddHostedService<OutboxRelayService>();

builder.Services.AddDbContext<InventoryDbContext>(options => {
    options.UseSqlite("Data Source=inventory.db");
});

builder.Services.AddSingleton<IProducer<string, StockReserved>>(sp =>
{
    var configuration = sp.GetRequiredService<IConfiguration>();
    var config = new ProducerConfig
    {
        BootstrapServers = configuration["Kafka:BootstrapServers"],
        EnableIdempotence = true,
        MessageTimeoutMs = 10000
    };

    var srclient = sp.GetRequiredService<ISchemaRegistryClient>();

    return new ProducerBuilder<string, StockReserved>(config)
            .SetValueSerializer(new AvroSerializer<StockReserved>(srclient,
                new AvroSerializerConfig() { SubjectNameStrategy = SubjectNameStrategy.TopicRecord })
                .AsSyncOverAsync())
            .Build();
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