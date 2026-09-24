namespace OrderFlow.Notification;

public interface IEmailSender
{
    Task SendEmail(string orderId, string eventName);
}