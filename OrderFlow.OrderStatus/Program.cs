using Avro.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using OrderFlow.Domain;
using OrderFlow.OrderStatus;
using Serilog;



var builder = Host.CreateApplicationBuilder(args);

Log.Logger = new LoggerConfiguration()
    .ReadFrom.Configuration(builder.Configuration)
    .CreateLogger();
builder.Logging.ClearProviders();
builder.Services.AddSerilog();

builder.Services.AddHostedService<OrderStatusService>();

builder.Services.AddDbContext<OrdersProjectionDbContext>(opt =>
{
    opt.UseSqlite("Data Source=../Data/ordersprojection.db");
});

builder.Services.AddSingleton<IProducer<byte[], byte[]>>(sp =>
{
    var configuration = sp.GetRequiredService<IConfiguration>();
    var config = new ProducerConfig
    {
        BootstrapServers = configuration["Kafka:BootstrapServers"],
        EnableIdempotence = true
    };

    return new ProducerBuilder<byte[], byte[]>(config).Build();
});

builder.Services.AddSingleton<ISchemaRegistryClient>(sp =>
{
    var configuration = sp.GetRequiredService<IConfiguration>();
    return new CachedSchemaRegistryClient(new SchemaRegistryConfig() { Url = configuration["SchemaRegistry:Url"] });
});

builder.Services.AddSingleton<AvroSerializer<GenericRecord>>(sp =>
{
    var srclient = sp.GetRequiredService<ISchemaRegistryClient>();
    return new AvroSerializer<GenericRecord>(srclient, new AvroSerializerConfig
    {
        SubjectNameStrategy = SubjectNameStrategy.TopicRecord
    });
});

var host = builder.Build();
using (var scope = host.Services.CreateScope())
{
    scope.ServiceProvider.GetRequiredService<OrdersProjectionDbContext>().Database.EnsureCreated();
}
host.Run();
