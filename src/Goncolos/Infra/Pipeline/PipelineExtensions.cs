using System;
using System.Threading.Tasks;

namespace Goncolos.Infra.Pipeline
{
    public static class PipelineExtensions
    {
        public static PipelineBuilder<TContext> Use<TContext>(this PipelineBuilder<TContext> builder, Func<TContext, Func<Task>, Task> step)
            where TContext : IPipelineContext
        {
            return builder
                .Use(next =>
            {
                return context =>
                {
                    Func<Task> simpleNext = () => next(context);
                    return step(context, simpleNext);
                };
            });
        }
        
        public static PipelineBuilder<TContext> Use<TContext>(this PipelineBuilder<TContext> builder, Func<TContext, Func<TContext,Task>, Task> step)
            where TContext : IPipelineContext
        {
            return builder
                .Use(next =>
            {
                return context =>
                {
                    return step(context,c=>next(c));
                };
            });
        }
        
        
        public static PipelineBuilder<TContext> UsePipelineStepWhen<TContext>(this PipelineBuilder<TContext> builder, Func<TContext,bool> condition,IPipelineStep<TContext> step)
            where TContext : IPipelineContext
        {
            return builder.Use(next =>
            {
                return async context =>
                {
                    if (condition(context))
                    {
                        await step.Execute(context, next);
                    }
                    else
                    {
                        await next(context);
                    }
                };
            });
        }

        public static PipelineBuilder<TContext> UsePipelineStep<TContext>(this PipelineBuilder<TContext> builder, IPipelineStep<TContext> step)
            where TContext : IPipelineContext
        {
            return builder.Use(next => { return async context => { await step.Execute(context, next); }; });
        }
        
        public static PipelineBuilder<TContext> Map<TContext>(this PipelineBuilder<TContext> builder,  Func<TContext,bool> condition, Action<PipelineBuilder<TContext>> configuration)
            where TContext : IPipelineContext
        {
            var branchBuilder = builder.New();
            configuration(branchBuilder);
            var branch = branchBuilder.Build();

            var options = new MapOptions<TContext>
            {
                Condition = condition,
                Branch = branch,
            };
            return builder.Use(next => new MapPipelineStep<TContext>(next, options).Execute);
        }
    }

    public class
        MapOptions<TContext> where TContext : IPipelineContext
    {
        public Func<TContext, bool> Condition { get; set; }
        public Pipeline<TContext> Branch { get; set; }
    }
    
    public class MapPipelineStep<TContext> where TContext : IPipelineContext
    {
        private readonly PipelineStepDelegate<TContext> _next;
        private readonly MapOptions<TContext> _options;

        public MapPipelineStep(PipelineStepDelegate<TContext> next, MapOptions<TContext> options)
        {
            _next = next ?? throw new ArgumentNullException(nameof(next));
            _options = options ?? throw new ArgumentNullException(nameof(options));
        }

        public async Task Execute(TContext context)
        {
            if (_options.Condition(context))
            {
                await _options.Branch.Execute(context);
            }
            else
            {
                await _next(context);
            }
        }
    }
}