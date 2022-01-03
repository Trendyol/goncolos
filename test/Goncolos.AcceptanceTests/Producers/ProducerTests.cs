using System;
using System.Collections.Generic;
using System.Collections;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Goncolos.Producers;
using Shouldly;
using Xunit;
using Xunit.Abstractions;

namespace Goncolos.AcceptanceTests.Producers
{
    [Trait("Category", "it")]
    [Collection(nameof(KafkaSharedFixtureCollection))]
    public class ProducerTests
    {
        private readonly KafkaSharedFixture _fixture;
        private readonly ITestOutputHelper _testOutputHelper;

        public ProducerTests(KafkaSharedFixture fixture, ITestOutputHelper testOutputHelper)
        {
            _fixture = fixture;
            _testOutputHelper = testOutputHelper;
        }

        private string GetRandomTopicName()
        {
            return $"test-topic-{Guid.NewGuid().ToString("N").Substring(0, 10)}";
        }

        [Fact]
        public async Task should_produce_a_message()
        {
            var topicName = GetRandomTopicName();
            
            await _fixture.WithAdminClient(c => c.CreateTopicsAsync(new[]
            {
                new TopicSpecification()
                {
                    Name = topicName,
                    NumPartitions = 6,
                    ReplicationFactor = 1
                }
            }));

            var producer = new KafkaProducer(new KafkaProducerConfiguration(_fixture.KafkaBootstrapServers));
            var outgoingMessage = new OutgoingMessage(new TopicWithPartition(topicName), new byte[] {1, 2, 3, 4}, new Dictionary<string, string>()
                    {
                        ["x-header"] = "1"
                    }
                )
                .WithPartition(5)
                .WithKey("abc");
            await producer.Produce(outgoingMessage, default);

            using var kafkaConsumer = new ConsumerBuilder<string, byte[]>(new ConsumerConfig()
                {
                    BootstrapServers = _fixture.KafkaBootstrapServers,
                    GroupId = topicName,
                    AutoOffsetReset = AutoOffsetReset.Earliest
                })
                .SetValueDeserializer(Deserializers.ByteArray)
                .SetKeyDeserializer(Deserializers.Utf8)
                .Build();
            kafkaConsumer.Assign(new TopicPartitionOffset(topicName, new Partition(5),Offset.Beginning));
            while (true)
            {
                var result = kafkaConsumer.Consume(TimeSpan.FromSeconds(1));
                if (result == null)
                {
                    continue;
                }

                result.Message.Key.ShouldBe("abc");
                result.Message.ShouldNotBeNull();
                result.Message.Value.SequenceEqual(new byte[]{1,2,3,4}).ShouldBeTrue();
                break;
            }
        }


       
    }
}