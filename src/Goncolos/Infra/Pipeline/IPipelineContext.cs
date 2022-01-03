using System.Collections.Generic;

namespace Goncolos.Infra.Pipeline
{
    public interface IPipelineContext
    {
        IDictionary<object, object> Items { get; }
    }
}