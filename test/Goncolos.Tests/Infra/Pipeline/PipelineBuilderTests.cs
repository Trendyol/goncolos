using System.Threading.Tasks;
using Goncolos.Infra.Pipeline;
using Shouldly;
using Xunit;

namespace Goncolos.Tests.Infra.Pipeline
{
    public partial class PipelineBuilderTests
    {
        [Fact]
        public async Task should_execute_pipeline_step_object()
        {
            var pipeline = new PipelineBuilder<TestDataContext>()
                .UsePipelineStep(new TextWriterStep("hello"))
                .Build();
            var context = new TestDataContext();
            await pipeline.Execute(context);
            context.Text.ShouldBe("hello");
        }
        
        [Fact]
        public async Task should_execute_pipeline_step_delegate()
        {
            var pipeline = new PipelineBuilder<TestDataContext>()
                .Use(next =>
                {
                    return async ctx =>
                    {
                        ctx.Text = "1";
                        await next(ctx);
                    };
                })
                .Build();
            var context = new TestDataContext();
            await pipeline.Execute(context);
            context.Text.ShouldBe("1");
        }
        
        
        [Fact]
        public async Task should_execute_inner_pipeline_steps()
        {
            var pipeline = new PipelineBuilder<TestDataContext>()
                .Use(next =>
                {
                    return async ctx =>
                    {
                        ctx.Text = "hello";
                        await next(ctx);
                    };
                })
                .Use(next =>
                {
                    return async ctx =>
                    {
                        ctx.Text += " world";
                        await next(ctx);
                    };
                })
                .Build();
            var context = new TestDataContext();
            await pipeline.Execute(context);
            context.Text.ShouldBe("hello world");
        }
    }
}