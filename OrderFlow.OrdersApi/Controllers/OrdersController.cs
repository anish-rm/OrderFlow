using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using OrderFlow.Application.Dtos;
using OrderFlow.Application.Services;

namespace OrderFlow.OrdersApi.Controllers;

[Route("api/[controller]")]
[ApiController]
public class OrdersController(IOrdersService ordersService) :BaseController 
{
    [HttpPost]
    public async Task<ActionResult<OrderIdDto>> Placeorder(PlaceOrderRequestDto placeOrderRequestDto)
    {
        var result = await ordersService.PlaceOrderRequestAsync(placeOrderRequestDto);
        return ToActionResult(result);
    }
}

