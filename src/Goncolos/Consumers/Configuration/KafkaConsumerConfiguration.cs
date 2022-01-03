using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Goncolos.Consumers.Configuration
{
    public class KafkaConsumerConfiguration : ConsumerConfig
    {
        private readonly ConcurrentDictionary<string, SubscriptionOptions> _subscriptionOptions = new ConcurrentDictionary<string, SubscriptionOptions>();
        private Action<ConsumerBuilder<string, byte[]>> _consumerBuilderConfigurer;

        public KafkaConsumerConfiguration(string servers, string groupId)
        {
            if (string.IsNullOrEmpty(groupId))
            {
                throw new ArgumentNullException(nameof(servers));
            }

            BootstrapServers = servers;
            GroupId = groupId;
            AutoOffsetReset = Confluent.Kafka.AutoOffsetReset.Earliest;
            EnableAutoCommit = true;
            EnableAutoOffsetStore = false;
            AutoCommitIntervalMs = 2000;
            EnablePartitionEof = true;
            StatisticsIntervalMs = 0;
            ClientId = Constants.ClientId;
        }

        public ILogger Logger { get; private set; } = NullLogger.Instance;

        public IEnumerable<string> Topics => _subscriptionOptions.Keys;
        public OnConsumerDropped ConsumerDroppedHandler { get; private set; }
        public TimeSpan MaxDelayForPolling { get; set; } = TimeSpan.FromMilliseconds(100);
        public Func<ValueTask> ConsumerDisposedHandler { get; private set; }

        public KafkaConsumerConfiguration UseLogger(ILogger logger)
        {
            Logger = logger ?? NullLogger.Instance;
            return this;
        }

        internal SubscriptionOptions GetTopicOptions(TopicPartition topicPartition)
        {
            if (!_subscriptionOptions.TryGetValue(topicPartition.Topic, out var subscriptionOptions))
            {
                throw new ConfigurationException($"topic subscription options not found for : {topicPartition}");
            }

            return subscriptionOptions;
        }

        public KafkaConsumerConfiguration Subscribe(Action<SubscriptionOptions> configure)
        {
            var subscriptionOptions = new SubscriptionOptions();
            configure(subscriptionOptions);
            foreach (var topic in subscriptionOptions.Topics)
            {
                _subscriptionOptions[topic] = subscriptionOptions;
            }

            return this;
        }

        public KafkaConsumerConfiguration Configure(Action<KafkaConsumerConfiguration> configure)
        {
            if (configure == null)
            {
                throw new ArgumentNullException(nameof(configure));
            }

            configure(this);
            return this;
        }

        public KafkaConsumerConfiguration OnConsumerDropped(OnConsumerDropped handler)
        {
            ConsumerDroppedHandler = handler ?? ((consumer, exception) => Task.FromResult(RecoveryBehaviour.Retry));
            return this;
        }

        public KafkaConsumerConfiguration ConfigureConsumerBuilder(Action<ConsumerBuilder<string, byte[]>> configure)
        {
            _consumerBuilderConfigurer = configure;
            return this;
        }

        protected internal virtual ConsumerBuilder<string, byte[]> CreateConsumerBuilder()
        {
            var builder = new ConsumerBuilder<string, byte[]>(this)
                .SetValueDeserializer(Deserializers.ByteArray)
                .SetKeyDeserializer(Deserializers.Utf8)
                .SetErrorHandler(ErrorHandler)
                .SetStatisticsHandler(StatisticsHandler);
            _consumerBuilderConfigurer?.Invoke(builder);
            return builder;
        }

        private void StatisticsHandler(IConsumer<string, byte[]> c, string data)
        {
            Logger.LogTrace(data);
        }

        private void ErrorHandler(IConsumer<string, byte[]> c, Error error)
        {
            if (error.IsFatal)
            {
                Logger.LogError("Kafka consumer fatal error, {@error}", error);
            }
            else
            {
                Logger.LogWarning("Kafka consumer error, {@error}", error);
            }
        }
    }
}