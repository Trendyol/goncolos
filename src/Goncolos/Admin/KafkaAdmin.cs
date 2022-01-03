using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Logging;

namespace Goncolos.Admin
{
    public class KafkaAdmin
        : IDisposable
    {
        private readonly KafkaAdminConfiguration _configuration;
        private readonly IAdminClient _adminClient;

        public KafkaAdmin(KafkaAdminConfiguration configuration)
        {
            _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
            var builder = configuration.CreateAdminClientBuilder();
            _adminClient = builder.Build();
        }

        public async Task DeleteTopic(string topicName)
        {
            await _adminClient.DeleteTopicsAsync(new[] {topicName}, new DeleteTopicsOptions()
            {
                OperationTimeout = _configuration.OperationTimeout,
                RequestTimeout = _configuration.RequestTimeout
            });
        }

        public async Task CreateTopic(TopicSpecification topicSpecification, bool suppressErrorIfExists = true)
        {
            try
            {
                await _adminClient.CreateTopicsAsync(new[]
                {
                    topicSpecification
                }, new CreateTopicsOptions()
                {
                    OperationTimeout = _configuration.OperationTimeout,
                    RequestTimeout = _configuration.RequestTimeout
                });
            }
            catch (CreateTopicsException e) when (suppressErrorIfExists && e.Message.Contains("already exists"))
            {
                _configuration.Logger.LogDebug(e, $"An error occurred when creating topic: {e.Message}, topic: {topicSpecification.Name}");
            }
        }

        public void Dispose()
        {
            _adminClient?.Dispose();
        }
    }
}