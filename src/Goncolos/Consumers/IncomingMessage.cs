using System;
using System.Collections.Generic;

namespace Goncolos.Consumers
{
    public class IncomingMessage
    {
        public IncomingMessage(TopicWithPartition topic, long offset, Dictionary<string, string> headers,
            ReadOnlyMemory<byte> body, string key, DateTimeOffset timestamp)
        {
            Key = key;
            Body = body;
            Timestamp = timestamp;
            Headers = headers != null
                ? new Dictionary<string, string>(headers, StringComparer.InvariantCultureIgnoreCase)
                : new Dictionary<string, string>(StringComparer.InvariantCultureIgnoreCase);
            Topic = topic ?? throw new ArgumentNullException(nameof(topic));
            Offset = offset;
        }

        public string Key { get; }
        public ReadOnlyMemory<byte> Body { get; }
        public Dictionary<string, string> Headers { get; }
        public TopicWithPartition Topic { get; }
        public long Offset { get; }
        public DateTimeOffset Timestamp { get; }

        public override string ToString()
        {
            return $"key={Key}, topic={Topic}, offset={Offset}, timestamp={Timestamp}";
        }
    }
}