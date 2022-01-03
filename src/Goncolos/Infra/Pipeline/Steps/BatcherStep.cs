using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Goncolos.Infra.Pipeline.Steps
{
    public abstract class BatcherStep<TContext>
        : IPipelineStep<TContext> where TContext : IPipelineContext
    {
        private readonly int _queueSizePerType;

        private readonly ConcurrentDictionary<string, ConcurrentQueue<QueuedStep>> _stepQueues
            = new ConcurrentDictionary<string, ConcurrentQueue<QueuedStep>>();

        protected BatcherStep(int queueSizePerType = 100)
        {
            _queueSizePerType = queueSizePerType;
        }

        protected virtual string GetQueueName(TContext context)
        {
            var messageType = context;
            return messageType.ToString();
        }

        public async Task Execute(TContext context, PipelineStepDelegate<TContext> next)
        {
            var queueName = GetQueueName(context);
            if (!_stepQueues.TryGetValue(queueName, out var q))
            {
                q = new ConcurrentQueue<QueuedStep>();
                _stepQueues.TryAdd(queueName, q);
            }

            q.Enqueue(new QueuedStep(context, next));

            if (q.Count == _queueSizePerType)
            {
                var steps = new List<QueuedStep>();
                while (q.TryDequeue(out var m))
                {
                    steps.Add(m);
                }

                await ExecuteQueuedSteps(steps.ToArray());
            }
        }

        protected virtual async Task ExecuteQueuedSteps(QueuedStep[] steps)
        {
            foreach (var step in steps)
            {
                await step.Next(step.Context);
            }
        }

        protected readonly struct QueuedStep
        {
            public readonly TContext Context;
            public readonly PipelineStepDelegate<TContext> Next;

            public QueuedStep(TContext context, PipelineStepDelegate<TContext> next)
            {
                Context = context;
                Next = next;
            }
        }
    }
}