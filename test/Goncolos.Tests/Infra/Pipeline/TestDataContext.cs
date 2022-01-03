using System.Collections.Generic;
using Goncolos.Infra.Pipeline;

namespace Goncolos.Tests.Infra.Pipeline
{
    public class TestDataContext : IPipelineContext
    {
        public IDictionary<object, object> Items { get; } = new Dictionary<object, object>();

        public string Text
        {
            get => Items.TryGetValue("Text", out var item) ? item?.ToString() : null;
            set => Items["Text"] = value;
        }

        public int IntValue { get; set; }

    }
}