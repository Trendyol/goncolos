using System;
using System.Collections.Generic;
using System.Threading;
using Goncolos.Consumers;
using Goncolos.Infra.Pipeline;

namespace Goncolos.HighLevel.Consumers.Single
{
    public class SingleIncomingMessageContext : IPipelineContext
    {
        public IDictionary<object, object> Items { get; }
        public object Message { get; set; }
        public IncomingMessage IncomingMessage { get; }
        public CancellationToken CancellationToken { get; }

        public SingleIncomingMessageContext(IncomingMessage incomingMessage, CancellationToken cancellationToken = default,IDictionary<object, object> items=null)
        {
            IncomingMessage = incomingMessage ?? throw new ArgumentNullException(nameof(incomingMessage));
            CancellationToken = cancellationToken;
            Items = items ?? new Dictionary<object, object>();
        }
    }
}