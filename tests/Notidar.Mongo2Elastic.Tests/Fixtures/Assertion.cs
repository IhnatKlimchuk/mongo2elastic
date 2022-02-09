using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Notidar.Mongo2Elastic.Tests.Fixtures
{
    public static class Assertion
    {
        public static async Task Eventually(Func<Task> callback, TimeSpan? waitTime = null, TimeSpan? pollInterval = null)
        {
            waitTime ??= TimeSpan.FromSeconds(60);
            pollInterval ??= TimeSpan.FromSeconds(1);

            if (waitTime < TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(nameof(waitTime),
                    $"The value of {nameof(waitTime)} must be non-negative.");
            }

            if (pollInterval < TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(nameof(pollInterval),
                    $"The value of {nameof(pollInterval)} must be non-negative.");
            }

            TimeSpan? invocationEndTime = null;
            Exception lastException = null;
            var timer = Stopwatch.StartNew();

            while (invocationEndTime is null || invocationEndTime < waitTime)
            {
                try
                {
                    await callback();
                    return;
                }
                catch (Exception ex)
                {
                    lastException = ex;
                }

                await Task.Delay(pollInterval.Value);
                invocationEndTime = timer.Elapsed;
            }

            throw lastException;
        }

        public static async Task Eventually(Action callback, TimeSpan? waitTime = null, TimeSpan? pollInterval = null)
        {
            Task Func()
            {
                callback();
                return Task.CompletedTask;
            }

            await Eventually(Func, waitTime, pollInterval);
        }
    }
}
