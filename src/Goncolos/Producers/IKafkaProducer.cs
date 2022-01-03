using System;
using System.Threading;
using System.Threading.Tasks;

namespace Goncolos.Producers
{
    public interface IKafkaProducer : IDisposable
    {
        Task ProduceBulkWithCallback(OutgoingMessage[] messages);
        Task ProduceBulk(OutgoingMessage[] messages, CancellationToken cancellationToken = default);
        ValueTask Produce(OutgoingMessage message, CancellationToken cancellationToken);
    }
}