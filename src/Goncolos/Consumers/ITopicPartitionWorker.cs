using System;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Goncolos.Consumers
{
    public interface ITopicPartitionWorker : IAsyncDisposable
    {
        ValueTask Enqueue(ConsumeResult<string, byte[]> result);
    }
}