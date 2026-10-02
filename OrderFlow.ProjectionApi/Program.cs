using Microsoft.EntityFrameworkCore;
using OrderFlow.Domain;
using OrderFlow.ProjectionApi.Contracts;
using OrderFlow.ProjectionApi.Services;
using System.Text.Json.Serialization;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddDbContext<OrdersProjectionDbContext>(opt =>
{
    opt.UseSqlite("Data Source=../Data/ordersprojection.db");
});

builder.Services.AddScoped<IOrderStatusService, OrderStatusService>();

builder.Services.AddControllers()
       .AddJsonOptions(opt =>
       {
           opt.JsonSerializerOptions.Converters.Add(new JsonStringEnumConverter());
           opt.JsonSerializerOptions.ReferenceHandler = System.Text.Json.Serialization.ReferenceHandler.IgnoreCycles;
       });

var app = builder.Build();

// Configure the HTTP request pipeline.

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.Run();
