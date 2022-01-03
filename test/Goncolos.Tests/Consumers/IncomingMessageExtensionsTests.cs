using System.Collections.Generic;
using System.Text;
using Goncolos.Consumers;
using Goncolos.HighLevel;
using Goncolos.Infra;
using Shouldly;
using Xunit;

namespace Goncolos.Tests.Consumers
{
    public class IncomingMessageExtensionsTests
    {
        [Fact]
        public void get_return_header_value_as_datetimeoffset()
        {
            var now = SystemTime.UtcNowOffset;
            var message = CreateIncomingMessage();
            message.Headers["x-date"] = now.ToString("O");
            
            message.TryGetHeaderAsDateTimeOffset("x-date",out var dt).ShouldBeTrue();
            dt.ShouldBe(now);
        }
        
        [Fact]
        public void get_return_header_value_as_int()
        {
            var message = CreateIncomingMessage();
            message.Headers["x-int"] = "3";
            
            message.TryGetHeaderAsInt("x-int",out var v).ShouldBeTrue();
            v.ShouldBe(3);
        } 
        
        [Fact]
        public void get_return_null_as_default_value_when_header_is_null()
        {
            var message = CreateIncomingMessage();
            message.GetHeaderOrDefault("x-v").ShouldBeNull();
        }
        
        [Fact]
        public void get_return_default_value_when_header_is_null()
        {
            var message = CreateIncomingMessage();
            message.GetHeaderOrDefault("x-v",()=>"default value").ShouldBe("default value");
        }

        private static IncomingMessage CreateIncomingMessage()
        {
            return new IncomingMessageBuilder
            {
                Body = Encoding.UTF8.GetBytes("hello world"),
                Key = "test-key",
                Topic = new TopicWithPartition("test-topic", 1)
            }.Build();
        }
    }
}