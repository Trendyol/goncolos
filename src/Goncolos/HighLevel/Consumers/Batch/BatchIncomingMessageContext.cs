using System.Collections.Generic;
using System.Threading;
using Goncolos.Consumers;
using Goncolos.Infra.Pipeline;

namespace Goncolos.HighLevel.Consumers.Batch
{
    public class BatchIncomingMessageContext : IPipelineContext
    {
        public IDictionary<object, object> Items { get; } 
        public IncomingMessage[] IncomingMessages{ get; }
        public CancellationToken CancellationToken { get; }
        public bool IsConsumerClosed => CancellationToken.IsCancellationRequested;
        public BatchIncomingMessageContext(IncomingMessage[] incomingMessages, CancellationToken cancellationToken=default,IDictionary<object, object> items=null)
        {
            IncomingMessages = incomingMessages;
            CancellationToken = cancellationToken;
            Items = items ?? new Dictionary<object, object>();
        }
    }
}