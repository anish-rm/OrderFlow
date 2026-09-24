namespace OrderFlow.Common;

public static class Retry
{
    public static async Task ExecuteWithRetry(Func<Task> work, Func<Exception, bool> isTransient, int budget = 3,
        int baseDelayMs = 1000)
    {
        Exception lastException = null!;
        for (int attempt = 1; attempt <= budget; attempt++)
        {
            try
            {
                await work();
                return;
            }
            catch (Exception ex) when (isTransient(ex))
            {
                lastException = ex;
                if (attempt == budget) break;
                
                var delay = baseDelayMs * (int)Math.Pow(2, attempt - 1);
                Thread.Sleep(Random.Shared.Next(delay - 200, delay + 200));
            }
        }
        
        throw new RetryExhaustedException(budget, lastException);
    }
}