using System;
using System.Collections.Generic;
using Goncolos.Consumers;
using Shouldly;
using Xunit;

namespace Goncolos.Tests.Consumers
{
    public class IncomingMessageTests
    {
        [Fact]
        public void should_initialize_message()
        {
            var now = DateTimeOffset.UtcNow;
            var message = new IncomingMessage(
                new TopicWithPartition("abc", 3),
                1,
                new Dictionary<string, string>()
                {
                    ["abc"] = "def"
                },
                new byte[] {1, 2, 3},
                "test",now
            );
            
            message.Topic.ShouldBe(new TopicWithPartition("abc",3));
            message.Offset.ShouldBe(1);
            message.Headers.ShouldBe(new Dictionary<string, string>()
            {
                ["abc"]="def"
            });
            message.Body.ShouldBe(new byte[]{1,2,3});
            message.Key.ShouldBe("test");
            message.Timestamp.ShouldBe(now);
        }
        
        
    }

    public static class AssertExtensions
    {
        public static void ShouldBeEqual(this IncomingMessage actual, IncomingMessage expected)
        {
            if (ReferenceEquals(actual, expected)) return;
            actual.Topic.ShouldBe(expected.Topic);
            actual.Offset.ShouldBe(expected.Offset);
            actual.Headers.ShouldBe(expected.Headers);
            actual.Body.ShouldBe(expected.Body);
            actual.Key.ShouldBe(expected.Key);
            actual.Timestamp.ShouldBe(expected.Timestamp);
        }
    }
}