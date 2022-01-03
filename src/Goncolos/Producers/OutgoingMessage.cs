using System;
using System.Collections.Generic;
using Goncolos.Infra;

namespace Goncolos.Producers
{
    public class OutgoingMessage
    {
        public TopicWithPartition Topic { get; private set; }

        public ReadOnlyMemory<byte> Body { get; }

        public Dictionary<string, string> Headers { get; }

        public string Key { get; private set; }

        public OutgoingMessage(TopicWithPartition topic, ReadOnlyMemory<byte> body, Dictionary<string, string> headers)
        {
            Topic = topic ?? throw new ArgumentNullException(nameof(topic));
            Body = body ;
            Headers = headers != null
                ? new Dictionary<string, string>(headers, StringComparer.InvariantCultureIgnoreCase)
                : new Dictionary<string, string>(StringComparer.InvariantCultureIgnoreCase);
        }

        public string GetHeaderOrDefault(string key, Func<string> defaultValue = null)
        {
            return Headers.TryGetValue(key, out var v) ? v : defaultValue?.Invoke();
        }

        public OutgoingMessage SetHeader(string key, string value)
        {
            Headers[key] = value;
            return this;
        }

        public OutgoingMessage WithPartition(int partition)
        {
            Topic = Topic.WithPartition(partition);
            return this;
        }

        public OutgoingMessage WithKey(string key)
        {
            Key = key;
            return this;
        }
    }
}