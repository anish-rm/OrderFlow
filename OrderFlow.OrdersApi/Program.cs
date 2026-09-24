using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using OrderFlow.Application.Services;
using OrderFlow.Contracts;
using Serilog;

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .Enrich.FromLogContext()
    .WriteTo.Console()
    .CreateBootstrapLogger();

try
{
    Log.Information("Starting up..");
    var builder = WebApplication.CreateBuilder(args);
    builder.Host.UseSerilog((context, services, configuration) => configuration
            .ReadFrom.Configuration(context.Configuration)
            .ReadFrom.Services(services) 
    ); 

// Add services to the container.
    builder.Services.AddSingleton<IProducer<string, OrderPlaced>>(sp =>
    {
        var srClient = sp.GetRequiredService<ISchemaRegistryClient>();
        var configuration = sp.GetRequiredService<IConfiguration>();
        var config = new ProducerConfig
        {
            BootstrapServers = configuration["Kafka:BootstrapServers"],
            EnableIdempotence = true,
            MessageTimeoutMs = 10000,
            Acks = Acks.All,
        };
        return new ProducerBuilder<string, OrderPlaced>(config)
            .SetValueSerializer(new AvroSerializer<OrderPlaced>(srClient,
                new AvroSerializerConfig{SubjectNameStrategy = SubjectNameStrategy.TopicRecord})
                .AsSyncOverAsync())
            .Build();
    });
    
    builder.Services.AddSingleton<ISchemaRegistryClient>(sp =>
    {
        var configuration = sp.GetRequiredService<IConfiguration>();
        return new CachedSchemaRegistryClient(new SchemaRegistryConfig
        {
            Url = configuration["SchemaRegistry:Url"]
        });
    });
    

   
    builder.Services.AddScoped<IOrdersService, OrdersService>();
    // builder.Services.AddScoped<IPaymentsService, PaymentsService>();
    
    builder.Services.AddHttpContextAccessor(); 

    builder.Services.AddControllers();
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
    builder.Services.AddOpenApi();

    var app = builder.Build();

    _ = app.Services.GetRequiredService<IProducer<string, OrderPlaced>>();

// Configure the HTTP request pipeline.
    if (app.Environment.IsDevelopment())
    {
        app.MapOpenApi();
    }

    app.UseHttpsRedirection();

    app.UseAuthorization();

    app.MapControllers();

    app.Lifetime.ApplicationStopping.Register(() =>
        app.Services.GetRequiredService<IProducer<string, OrderPlaced>>()
            .Flush(TimeSpan.FromSeconds(10)));

    app.Run();
}
catch (Exception ex)
{
    Log.Fatal(ex, "Application failed to start.");
}
finally
{
    Log.Information("Application shutdown.");
    Log.CloseAndFlush();
}
