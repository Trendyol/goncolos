using System.Threading.Tasks;

namespace Goncolos.Infra.Pipeline
{
    public class Pipeline<TContext>
    {
        private readonly PipelineStepDelegate<TContext> _pipelineStepDelegate;

        public Pipeline(PipelineStepDelegate<TContext> pipelineStepDelegate)
        {
            _pipelineStepDelegate = pipelineStepDelegate;
        }

        public async Task Execute(TContext context)
        {
            await _pipelineStepDelegate(context);
        }
    }
}