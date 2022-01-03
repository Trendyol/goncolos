using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Goncolos.Infra.Pipeline
{
    public class PipelineBuilder<TContext> where TContext : IPipelineContext
    {
        private readonly IList<Func<PipelineStepDelegate<TContext>, PipelineStepDelegate<TContext>>> _steps
            = new List<Func<PipelineStepDelegate<TContext>, PipelineStepDelegate<TContext>>>();


        public PipelineBuilder<TContext> Use(Func<PipelineStepDelegate<TContext>, PipelineStepDelegate<TContext>> step)
        {
            _steps.Add(step);
            return this;
        }

        public Pipeline<TContext> Build()
        {
            PipelineStepDelegate<TContext> step = context => Task.CompletedTask;
            for (var s = _steps.Count - 1; s >= 0; s--)
            {
                step = _steps[s](step);
            }

            return new Pipeline<TContext>(step);
        }

        public PipelineBuilder<TContext> New()
        {
            return new PipelineBuilder<TContext>();
        }
    }
}