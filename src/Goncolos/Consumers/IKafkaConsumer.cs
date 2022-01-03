using System;
using System.Threading;
using System.Threading.Tasks;

namespace Goncolos.Consumers
{
    public interface IKafkaConsumer : IAsyncDisposable
    {
        string Name { get; }
        Task Start(CancellationToken cancellationToken=default);
    }
}