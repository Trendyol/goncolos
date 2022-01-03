using System;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka.Admin;
using Goncolos.Admin;
using Shouldly;
using Xunit;
using Xunit.Abstractions;

namespace Goncolos.AcceptanceTests.Admin
{
    [Trait("Category", "it")]
    [Collection(nameof(KafkaSharedFixtureCollection))]
    public class KafkaAdminTests
    {
        public KafkaAdminTests(KafkaSharedFixture fixture, ITestOutputHelper testOutputHelper)
        {
            _fixture = fixture;
            _testOutputHelper = testOutputHelper;
        }

        private readonly KafkaSharedFixture _fixture;
        private readonly ITestOutputHelper _testOutputHelper;

        private string GetRandomTopicName()
        {
            return $"test-topic-{Guid.NewGuid().ToString("N").Substring(0, 10)}";
        }

        [Fact]
        public async Task should_create_topic()
        {
            var topicName = GetRandomTopicName();

            var admin = new KafkaAdmin(new KafkaAdminConfiguration(_fixture.KafkaBootstrapServers));
            await admin.CreateTopic(new TopicSpecification
            {
                Name = topicName,
                NumPartitions = 10,
                ReplicationFactor = 1
            });

            await _fixture.WithAdminClient(client =>
            {
                var data = client.GetMetadata(topicName, TimeSpan.FromSeconds(1));
                data.ShouldNotBeNull();
                data.Topics.FirstOrDefault().Partitions.Count.ShouldBe(10);
                return Task.CompletedTask;
            });
        }

        [Fact]
        public async Task should_delete_topic()
        {
            var topicName = GetRandomTopicName();

            var admin = new KafkaAdmin(new KafkaAdminConfiguration(_fixture.KafkaBootstrapServers));
            await admin.CreateTopic(new TopicSpecification
            {
                Name = topicName,
                NumPartitions = 10,
                ReplicationFactor = 1
            });

            await admin.DeleteTopic(topicName);

            await _fixture.WithAdminClient(client =>
            {
                var data = client.GetMetadata(topicName, TimeSpan.FromSeconds(1));
                var topicMetaData = data.Topics.FirstOrDefault(t => t.Topic == topicName);
                topicMetaData?.Partitions.Count.ShouldBe(0);
                return Task.CompletedTask;
            });
        }

        [Fact]
        public async Task should_suppress_error_if_topic_exists()
        {
            var topicName = GetRandomTopicName();

            var admin = new KafkaAdmin(new KafkaAdminConfiguration(_fixture.KafkaBootstrapServers));
            await admin.CreateTopic(new TopicSpecification
            {
                Name = topicName,
                NumPartitions = 10,
                ReplicationFactor = 1
            }, true);

            await admin.CreateTopic(new TopicSpecification
            {
                Name = topicName,
                NumPartitions = 10,
                ReplicationFactor = 1
            }).ShouldNotThrowAsync();

            await _fixture.WithAdminClient(client =>
            {
                var data = client.GetMetadata(topicName, TimeSpan.FromSeconds(1));
                data.ShouldNotBeNull();
                data.Topics.FirstOrDefault().Partitions.Count.ShouldBe(10);
                return Task.CompletedTask;
            });
        }

        [Fact]
        public async Task should_throw_error_if_topic_exists()
        {
            var topicName = GetRandomTopicName();

            var admin = new KafkaAdmin(new KafkaAdminConfiguration(_fixture.KafkaBootstrapServers));
            await admin.CreateTopic(new TopicSpecification
            {
                Name = topicName,
                NumPartitions = 10,
                ReplicationFactor = 1
            });

            await admin.CreateTopic(new TopicSpecification
            {
                Name = topicName,
                NumPartitions = 10,
                ReplicationFactor = 1
            }, false).ShouldThrowAsync<CreateTopicsException>();
        }

        [Fact]
        public async Task should_throw_error_when_delete_non_existent_topic()
        {
            var topicName = GetRandomTopicName();

            var admin = new KafkaAdmin(new KafkaAdminConfiguration(_fixture.KafkaBootstrapServers));
            await admin.DeleteTopic(topicName).ShouldThrowAsync<DeleteTopicsException>();
        }
    }
}