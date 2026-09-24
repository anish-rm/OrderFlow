using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Microsoft.EntityFrameworkCore;
using OrderFlow.Domain;
using OrderFlow.Notification;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddDbContext<NotificationDbContext>(options =>
{
    options.UseSqlite("Data Source=notification.db");
});

builder.Services.AddSingleton<IProducer<byte[], byte[]>>(sp =>
{
    var configuration = sp.GetRequiredService<IConfiguration>();
    var config = new ProducerConfig()
    {
        BootstrapServers = configuration["Kafka:BootstrapServers"],
        EnableIdempotence = true,
        MessageTimeoutMs = 10000
    };
    return new ProducerBuilder<byte[], byte[]>(config).Build();
});

builder.Services.AddHostedService<NotificationConsumerService>();
builder.Services.AddScoped<IEmailSender, EmailSender>();

builder.Services.AddSingleton<ISchemaRegistryClient>(sp =>
{
    var configuration = sp.GetRequiredService<IConfiguration>();
    return new CachedSchemaRegistryClient(new SchemaRegistryConfig(){Url = configuration["SchemaRegistry:Url"]});
});

var host = builder.Build();
using (var scope = host.Services.CreateScope())
{
    scope.ServiceProvider.GetRequiredService<NotificationDbContext>().Database.EnsureCreated();
}
host.Run();