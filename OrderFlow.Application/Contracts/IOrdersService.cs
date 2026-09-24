using OrderFlow.Application.Dtos;
using OrderFlow.Common;

namespace OrderFlow.Application.Services;

public interface IOrdersService
{
    Task<Result<OrderIdDto>> PlaceOrderRequestAsync(PlaceOrderRequestDto placeOrderRequestDto);
}