using System.Threading.Tasks;

namespace Goncolos.Infra.Pipeline
{
    public interface IPipelineStep<TContext>
        where TContext : IPipelineContext
    {
        Task Execute(TContext context, PipelineStepDelegate<TContext> next);
    }
}