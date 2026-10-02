using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using OD = OrderFlow.Domain;
using OrderFlow.ProjectionApi.Contracts;


namespace OrderFlow.ProjectionApi.Controllers;

[Route("api/[controller]")]
[ApiController]
public class OrderStatusController(IOrderStatusService orderStatusService) : BaseController
{
    [HttpGet]
    public async Task<ActionResult<IEnumerable<OD.OrderStatus>>> GetAllOrdersStatus()
    {
        var result = await orderStatusService.GetAllOrdersStatusAsync();
        return ToActionResult(result);
    }

    [HttpGet("{id}")]
    public async Task<ActionResult<OD.OrderStatus>> GetOrderStatus(string id)
    {
        var result = await orderStatusService.GetOrdersStatusAsync(id);
        return ToActionResult(result);
    }

}
