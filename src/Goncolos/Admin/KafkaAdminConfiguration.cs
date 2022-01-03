using System;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Goncolos.Admin
{
    public class KafkaAdminConfiguration :AdminClientConfig
    {
        private Action<AdminClientBuilder> _adminClientBuilderConfigurer;
        public ILogger Logger { get; private set; } = NullLogger.Instance;
        public TimeSpan OperationTimeout { get; set; } = TimeSpan.FromSeconds(10);
        public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(10);
        
        public KafkaAdminConfiguration(string servers)
        {
            if (string.IsNullOrEmpty(servers))
            {
                throw new ArgumentNullException(nameof(servers));
            }
            
            BootstrapServers = servers;
            ClientId = Constants.ClientId;
        }
        
        public KafkaAdminConfiguration UseLogger(ILogger logger)
        {
            Logger = logger ?? NullLogger.Instance;
            return this;
        }
        
        public KafkaAdminConfiguration ConfigureProducerBuilder(Action<AdminClientBuilder> configure)
        {
            _adminClientBuilderConfigurer = configure;
            return this;
        }
        
        public KafkaAdminConfiguration Configure(Action<KafkaAdminConfiguration> configure)
        {
            if (configure == null)
            {
                throw new ArgumentNullException(nameof(configure));
            }

            configure(this);
            return this;
        }


        protected internal virtual AdminClientBuilder CreateAdminClientBuilder()
        {
            var builder = new AdminClientBuilder(this)
                .SetErrorHandler((c, e) => { Logger.LogWarning($"An error occurred on kafka admin client: reason={e.Reason}, code={e.Code}, fatal={e.IsFatal}, isBrokerError={e.IsBrokerError}", e); });
            _adminClientBuilderConfigurer?.Invoke(builder);
            return builder;
        }
    }
}