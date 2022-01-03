using System.Linq;
using System.Threading.Tasks;
using Goncolos.Infra.Pipeline;

namespace Goncolos.HighLevel.Consumers.Batch
{
    public class KeyBasedInMemoryCompactionStep
        :IPipelineStep<BatchIncomingMessageContext>
    {
        public KeyBasedInMemoryCompactionStep()
        {
        }
        public async Task Execute(BatchIncomingMessageContext context, PipelineStepDelegate<BatchIncomingMessageContext> next)
        {
            var compactedMessages = context.IncomingMessages
                .GroupBy(im => im.Key)
                .Select(g => g.OrderByDescending(m => m.Timestamp).FirstOrDefault())
                .ToArray();
            await next(new BatchIncomingMessageContext( compactedMessages, context.CancellationToken,context.Items));
        }
    }
}