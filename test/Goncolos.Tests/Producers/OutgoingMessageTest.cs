using System.Collections.Generic;
using Goncolos.Producers;
using Shouldly;
using Xunit;

namespace Goncolos.Tests.Producers
{
    public class OutgoingMessageTest
    {
        [Fact]
        public void should_set_and_get_header()
        {
            var message = new OutgoingMessage(
                new TopicWithPartition("topic", 3),
                new byte[] {1, 2},
                new Dictionary<string, string>()
            );
            message.WithKey("key");
            message.SetHeader("x-header", "value");

            message.GetHeaderOrDefault("x-header").ShouldBe("value");
            message.Key.ShouldBe("key");
            message.Topic.ShouldBe(new TopicWithPartition("topic", 3));
            message.Body.ShouldBe(new byte[] {1, 2});
        }
        
        [Fact]
        public void should_change_partition()
        {
            var message = new OutgoingMessage(
                new TopicWithPartition("topic", 3),
                new byte[] {1, 2},
                new Dictionary<string, string>()
            );
            message.WithPartition(5);
            message.Topic.ShouldBe(new TopicWithPartition("topic", 5));
        }
    }
}