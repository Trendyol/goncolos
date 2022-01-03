using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Goncolos.Consumers;
using Goncolos.Infra.Pipeline;

namespace Goncolos.HighLevel.Consumers.Single
{   
    public class IncomingMessageToPipelineExecutor
    {
        private readonly Pipeline<SingleIncomingMessageContext> _pipeline;
        private SemaphoreSlim _throttler;

        public IncomingMessageToPipelineExecutor(Pipeline<SingleIncomingMessageContext> pipeline, int maxDegreeOfParallelism = 1)
        {
            _pipeline = pipeline;
            WithMaxDegreeOfParallelism(maxDegreeOfParallelism);
        }

        public IncomingMessageToPipelineExecutor WithMaxDegreeOfParallelism(int maxDegreeOfParallelism = 1)
        {
            if (maxDegreeOfParallelism <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(maxDegreeOfParallelism));
            }

            if (maxDegreeOfParallelism > 1)
            {
                _throttler = new SemaphoreSlim(maxDegreeOfParallelism);
            }

            return this;
        }

        public async Task Execute(IncomingMessage[] incomingMessages, CancellationToken cancellationToken, IDictionary<object, object> items=null)
        {
            if (_throttler != null)
            {
                var allTasks = new List<Task>(incomingMessages.Length);
                foreach (var incomingMessage in incomingMessages.OrderBy(x => x.Offset))
                {
                    await _throttler.WaitAsync(cancellationToken);
                    allTasks.Add(
                        Task.Run(async () =>
                        {
                            try
                            {
                                var clonedItems = CloneItems(items); 
                                await _pipeline.Execute(new SingleIncomingMessageContext(incomingMessage, cancellationToken,clonedItems));
                            }
                            finally
                            {
                                _throttler.Release();
                            }
                        }, cancellationToken));
                }

                await Task.WhenAll(allTasks);
            }
            else
            {
                foreach (var message in incomingMessages.OrderBy(x => x.Offset))
                {
                    var clonedItems = CloneItems(items); 
                    await _pipeline.Execute(new SingleIncomingMessageContext(message, cancellationToken,clonedItems));
                }
            }
        }
        
        private Dictionary<object, object> CloneItems(IDictionary<object, object> dic)
        {
            return dic == null ? new Dictionary<object, object>() : new Dictionary<object, object>(dic);
        }
    }
}