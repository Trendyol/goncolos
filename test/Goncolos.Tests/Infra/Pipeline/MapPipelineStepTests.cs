using System;
using System.Threading.Tasks;
using Goncolos.Infra.Pipeline;
using Shouldly;
using Xunit;

namespace Goncolos.Tests.Infra.Pipeline
{
    public class MapPipelineStepTests
    {
        [Fact]
        public async Task should_execute_pipeline_when_condition_is_true()
        {
            var pipeline = new PipelineBuilder<TestDataContext>()
                .UsePipelineStepWhen(t=>t.IntValue == 1,new TextWriterStep("world"))
                .Build();
            var context = new TestDataContext {IntValue = 1};
            await pipeline.Execute(context);
            context.Text.ShouldBe("world");
        }
        
        [Fact]
        public async Task should_not_execute_pipeline_when_condition_is_false()
        {
            var pipeline = new PipelineBuilder<TestDataContext>()
                .UsePipelineStepWhen(t=>t.IntValue == 1,new TextWriterStep("world"))
                .Build();
            var context = new TestDataContext();
            await pipeline.Execute(context);
            context.Text.ShouldBeNull();
        }
        
        [Fact]
        public async Task should_execute_pipeline_with_explicit_next_call()
        {
             Task Step(TestDataContext context, Func<TestDataContext, Task> next)
             {
                 context.Text = "abc";
                 return next(context);
             }
            
            var pipeline = new PipelineBuilder<TestDataContext>()
                .Use(Step)
                .Build();
            var context = new TestDataContext();
            await pipeline.Execute(context);
            context.Text.ShouldBe("abc");
        }

      

        [Fact]
        public async Task should_not_be_deadlocked_when_there_is_not_alternative_branch()
        {
            var pipeline = new PipelineBuilder<TestDataContext>()
                .Map(t => t.IntValue == 1, helloBuilder =>
                {
                    helloBuilder
                        .UsePipelineStep(new TextWriterStep("one"));
                })
                .Build();
            var context = new TestDataContext()
            {
                IntValue = 1
            };
            await pipeline.Execute(context);
            context.IntValue.ShouldBe(1);
            context.Text.ShouldBe("one");

            context = new TestDataContext()
            {
                IntValue =2
            };
            await pipeline.Execute(context);
            context.IntValue.ShouldBe(2);
            context.Text.ShouldBeNull();
        }
        
        [Fact]
        public async Task should_execute_different_pipelines_in_different_branches()
        {
            var pipeline = new PipelineBuilder<TestDataContext>()
                .Map(t => t.IntValue == 1, helloBuilder =>
                {
                    helloBuilder
                        .UsePipelineStep(new TextWriterStep("one"));
                })
                .Use((ctx, next) =>
                {
                    ctx.Items["Finished"] = true;
                    return next();
                })
                .Build();
            var context = new TestDataContext()
            {
                IntValue = 1
            };
            await pipeline.Execute(context);
            context.IntValue.ShouldBe(1);
            context.Text.ShouldBe("one");
            context.Items.ContainsKey("Finished").ShouldBeFalse();

            context = new TestDataContext()
            {
                IntValue =2
            };
            await pipeline.Execute(context);
            context.IntValue.ShouldBe(2);
            context.Text.ShouldBeNull();
            context.Items["Finished"].ShouldBe(true);
        }
        
        [Fact]
        public async Task should_execute_different_child_pipelines_by_predicate()
        {
            var pipeline = new PipelineBuilder<TestDataContext>()
                .Use((ctx, next) =>
                {
                    ctx.Items["User"] = "xyz";
                    return next();
                })
                .Map(t => t.IntValue == 1, helloBuilder =>
                {
                    helloBuilder
                        .UsePipelineStep(new TextWriterStep("one"));
                })
                .Map(t => t.IntValue == 2, helloBuilder =>
                {
                    helloBuilder
                        .UsePipelineStep(new TextWriterStep("two"));
                })
                .Build();
            var context = new TestDataContext()
            {
                IntValue = 1
            };
            await pipeline.Execute(context);
            context.IntValue.ShouldBe(1);
            context.Text.ShouldBe("one");
            context.Items["User"].ShouldBe("xyz");

            context = new TestDataContext()
            {
                IntValue = 2
            };
            await pipeline.Execute(context);
            context.IntValue.ShouldBe(2);
            context.Text.ShouldBe("two");
            context.Items["User"].ShouldBe("xyz");
        }
    }
}