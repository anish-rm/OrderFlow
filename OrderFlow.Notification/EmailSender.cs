namespace OrderFlow.Notification;

public class EmailSender(ILogger<EmailSender>  logger) : IEmailSender
{
    public async Task SendEmail(string orderId, string eventName)
    {
        logger.LogInformation($"Sending email for {orderId}_{eventName}");
        await Task.Delay(2000);
    }
}