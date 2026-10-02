using Microsoft.EntityFrameworkCore;
using OrderFlow.Common;
using OrderFlow.Common.Constants;
using OrderFlow.ProjectionApi.Contracts;
using OD = OrderFlow.Domain;

namespace OrderFlow.ProjectionApi.Services;

public class OrderStatusService(OD.OrdersProjectionDbContext dbContext) : IOrderStatusService
{
    public async Task<Result<IEnumerable<OD.OrderStatus>>> GetAllOrdersStatusAsync()
    {
        try
        {
            var result = await dbContext.OrderStatuses.ToListAsync();
            return Result<IEnumerable<OD.OrderStatus>>.Success(result);
        }
        catch (Exception ex)
        {
            return Result<IEnumerable<OD.OrderStatus>>.Failure(new Error(ErrorCodes.Failure, ex.Message));
        }
    }

    public async Task<Result<OD.OrderStatus>> GetOrdersStatusAsync(string orderId)
    {
        try
        {
            var result = await dbContext.OrderStatuses
                            .Where(os => os.OrderId == orderId)
                            .FirstOrDefaultAsync();

            if (result == null)
            {
                return Result<OD.OrderStatus>.NotFound(new Error(ErrorCodes.NotFound, $"Order status for Id : ${orderId} not found"));
            }

            return Result<OD.OrderStatus>.Success(result);
        }
        catch (Exception ex)
        {
            return Result<OD.OrderStatus>.Failure(new Error(ErrorCodes.Failure, ex.Message));
        }
    }

}
