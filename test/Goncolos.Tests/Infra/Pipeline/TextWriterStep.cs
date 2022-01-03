using System.Threading.Tasks;
using Goncolos.Infra.Pipeline;

namespace Goncolos.Tests.Infra.Pipeline
{
    public class TextWriterStep : IPipelineStep<TestDataContext>
    {
        private readonly string _text;

        public TextWriterStep(string text)
        {
            _text = text;
        }

        public async Task Execute(TestDataContext dataContext, PipelineStepDelegate<TestDataContext> next)
        {
            dataContext.Text = _text;
            await next(dataContext);
        }
    }
}