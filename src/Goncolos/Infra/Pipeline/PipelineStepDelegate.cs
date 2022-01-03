using System.Threading.Tasks;

namespace Goncolos.Infra.Pipeline
{
    public delegate Task PipelineStepDelegate<in TContext>(TContext context);
}