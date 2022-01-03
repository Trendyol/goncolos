using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Goncolos.Infra.Pipeline.Steps;
using Shouldly;
using Xunit;

namespace Goncolos.Tests.Infra.Pipeline.Steps
{
    public class BatcherStepTests
    {
        [Fact]
        public async Task should_execute_single_messages_as_batch()
        {
            var step = new TestBatcherStep(2);
            var tasks = new List<Task>();
            for (int i = 0; i < 10; i++)
            {
                var i1 = i;
                tasks.Add(step.Execute(new TestDataContext()
                {
                    IntValue = i
                }, ctx => Next(ctx, i1)));
            }

            await Task.WhenAll(tasks);
        }

        private Task Next(TestDataContext context, int v)
        {
            context.IntValue.ShouldBe(v);
            return Task.CompletedTask;
        }


        public class TestBatcherStep : BatcherStep<TestDataContext>
        {
            public TestBatcherStep(int queueSizePerType = 2)
                : base(queueSizePerType)
            {
            }

            protected override async Task ExecuteQueuedSteps(QueuedStep[] steps)
            {
                steps.Length.ShouldBe(2);
                await Task.WhenAll(steps.Select(t => t.Next(t.Context)));
            }
        }
    }
}