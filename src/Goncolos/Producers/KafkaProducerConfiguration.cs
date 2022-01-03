using System;
using System.Linq;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Goncolos.Producers
{
    public class KafkaProducerConfiguration : ProducerConfig
    {
        private Action<ProducerBuilder<string, byte[]>> _producerBuilderConfigurer;
        private static readonly ErrorCode[] _swallowErrorCodes =
        {
            ErrorCode.Local_TimedOut,
            ErrorCode.Local_MsgTimedOut,
            ErrorCode.RequestTimedOut,
        };
        private static readonly Func<Error, bool> DefaultProducerInterruptCondition = error => error.IsFatal || !_swallowErrorCodes.Contains(error.Code);
        private  Func<Error, bool> _producerInterruptCondition =DefaultProducerInterruptCondition;
        public ILogger Logger { get; private set; } = NullLogger.Instance;

        public KafkaProducerConfiguration(string servers)
        {
            if (string.IsNullOrEmpty(servers))
            {
                throw new ArgumentNullException(nameof(servers));
            }

            BootstrapServers = servers;
            ClientId = Constants.ClientId;
        }
        
        public KafkaProducerConfiguration InterruptProducingWhenError(Func<Error, bool> condition)
        {
            _producerInterruptCondition = condition ?? DefaultProducerInterruptCondition;
            return this;
        }
        
         public bool ShouldInterrupt(Error error)
                {
                    return _producerInterruptCondition(error);
                }

        public KafkaProducerConfiguration UseLogger(ILogger logger)
        {
            Logger = logger ?? NullLogger.Instance;
            return this;
        }

        public KafkaProducerConfiguration ConfigureProducerBuilder(Action<ProducerBuilder<string, byte[]>> configure)
        {
            _producerBuilderConfigurer = configure;
            return this;
        }

        public KafkaProducerConfiguration Configure(Action<KafkaProducerConfiguration> configure)
        {
            if (configure == null)
            {
                throw new ArgumentNullException(nameof(configure));
            }

            configure(this);
            return this;
        }

        protected internal virtual ProducerBuilder<string, byte[]> CreateProducerBuilder()
        {
            var builder = new ProducerBuilder<string, byte[]>(this)
                .SetValueSerializer(Serializers.ByteArray)
                .SetKeySerializer(Serializers.Utf8)
                .SetErrorHandler((_, e) =>
                {
                    Logger.LogWarning($"An error occurred on kafka producer: {e.Reason}, fatal: {e.IsFatal}", e);
                });
            _producerBuilderConfigurer?.Invoke(builder);
            return builder;
        }
    }
}