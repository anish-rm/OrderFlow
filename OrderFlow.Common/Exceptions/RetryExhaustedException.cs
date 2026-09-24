namespace OrderFlow.Common;

public class RetryExhaustedException : Exception
{
    public int Attempts { get; }

    public RetryExhaustedException(int attempts, Exception lastException)
        : base($"Retry budgest exhausted after {attempts} attempts", lastException)
    {
        Attempts = attempts;
    } 
}