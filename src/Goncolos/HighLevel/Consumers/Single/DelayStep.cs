using System;
using System.Threading.Tasks;
using Goncolos.Consumers;
using Goncolos.Infra;
using Goncolos.Infra.Pipeline;

namespace Goncolos.HighLevel.Consumers.Single
{
    public class DelayStep
        : IPipelineStep<SingleIncomingMessageContext>
    {
        public async Task Execute(SingleIncomingMessageContext context, PipelineStepDelegate<SingleIncomingMessageContext> next)
        {
            if (!context.IncomingMessage.TryGetHeaderAsDateTimeOffset(Headers.PublishedAt, out var publishedAt))
            {
                await next(context);
                return;
            }

            if (!context.IncomingMessage.TryGetHeaderAsInt(Headers.DelaySeconds, out var delaySeconds))
            {
                await next(context);
                return;
            }

            var now = SystemTime.UtcNowOffset;
            var delayTo = publishedAt + TimeSpan.FromSeconds(delaySeconds);
            var diffToDelay = delayTo - now;
            if (diffToDelay > TimeSpan.Zero)
            {
                await Task.Delay(diffToDelay, context.CancellationToken);
            }

            await next(context);
        }
    }
}