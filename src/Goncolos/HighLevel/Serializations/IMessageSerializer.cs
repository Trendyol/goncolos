using System.Collections.Generic;
using System.Threading.Tasks;
using Goncolos.Consumers;
using Goncolos.Producers;

namespace Goncolos.HighLevel.Serializations
{
    public interface IMessageSerializer
    {
        ValueTask<object> Deserialize(IncomingMessage incomingMessage);
        ValueTask<OutgoingMessage> Serialize(object message, TopicWithPartition topic, Dictionary<string, string> headers = null);
    }
}