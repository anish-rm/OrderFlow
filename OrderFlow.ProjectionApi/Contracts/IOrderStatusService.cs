using OrderFlow.Common;

namespace OrderFlow.ProjectionApi.Contracts
{
    public interface IOrderStatusService
    {
        Task<Result<IEnumerable<Domain.OrderStatus>>> GetAllOrdersStatusAsync();
        Task<Result<Domain.OrderStatus>> GetOrdersStatusAsync(string orderId);
    }
}