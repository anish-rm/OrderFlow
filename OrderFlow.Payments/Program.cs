using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.EntityFrameworkCore;
using OrderFlow.Contracts;
using OrderFlow.Domain;
using OrderFlow.Payments;
using OrderFlow.Payments.Services;
using Serilog;

var builder = Host.CreateApplicationBuilder(args);

Log.Logger = new LoggerConfiguration()
    .ReadFrom.Configuration(builder.Configuration)
    .CreateLogger();
builder.Logging.ClearProviders();
builder.Services.AddSerilog();

builder.Services.AddHostedService<PaymentConsumerService>();
builder.Services.AddHostedService<OutboxRelayService>();

builder.Services.AddDbContext<PaymentsDbContext>(options => {
    options.UseSqlite("Data Source=payments.db");
});

builder.Services.AddScoped<IPaymentsService, PaymentsService>();

builder.Services.AddSingleton<IProducer<string, PaymentReceived>>(sp =>
{
    var configuration = sp.GetRequiredService<IConfiguration>();
    var srclient = sp.GetRequiredService<ISchemaRegistryClient>();
    var config = new ProducerConfig
    {
        BootstrapServers = configuration["Kafka:BootstrapServers"],
        EnableIdempotence = true,
        MessageTimeoutMs = 10000,
        Acks = Acks.All
    };
    return new ProducerBuilder<string, PaymentReceived>(config)
        .SetValueSerializer(new AvroSerializer<PaymentReceived>(srclient,
            new AvroSerializerConfig{SubjectNameStrategy = SubjectNameStrategy.TopicRecord})
            .AsSyncOverAsync())
        .Build();
});

builder.Services.AddSingleton<ISchemaRegistryClient>(sp =>
{
    var configuration = sp.GetRequiredService<IConfiguration>();
    return new CachedSchemaRegistryClient(new SchemaRegistryConfig()
    {
        Url = configuration["SchemaRegistry:Url"],
    });
});

builder.Services.AddSingleton<IProducer<byte[], byte[]>>(sp =>
{
    var configuration = sp.GetRequiredService<IConfiguration>();
    var config = new ProducerConfig
    {
        BootstrapServers = configuration["Kafka:BootstrapServers"],
        EnableIdempotence = true,
        MessageTimeoutMs = 10000
    };
    return new ProducerBuilder<byte[], byte[]>(config).Build();
});

builder.Services.AddSingleton<ISchemaRegistryClient>(sp =>
{
    var configuration = sp.GetRequiredService<IConfiguration>();
    return new CachedSchemaRegistryClient(new SchemaRegistryConfig
    {
        Url = configuration["SchemaRegistry:Url"]
    });
});


var host = builder.Build();

using (var scope = host.Services.CreateScope())
{
    scope.ServiceProvider.GetRequiredService<PaymentsDbContext>().Database.EnsureCreated();
}

host.Run();