using System;
using System.Threading.Tasks;

namespace Goncolos.Consumers
{
    public delegate Task<RecoveryBehaviour> OnConsumerDropped(IKafkaConsumer consumer, Exception exception);

    public enum RecoveryBehaviour
    {
        Stop,
        Retry
    }
}