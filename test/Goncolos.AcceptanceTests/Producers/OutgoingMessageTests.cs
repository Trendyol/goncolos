using System.Collections.Generic;
using Goncolos.Producers;
using Shouldly;
using Xunit;

namespace Goncolos.AcceptanceTests.Producers
{
    public class OutgoingMessageTests
    {
        [Fact]
        public void should_equals_work_correctly()
        {
            var actual = new OutgoingMessage("topic", new byte[] {1, 2, 3}, new Dictionary<string, string>()
            {
                ["header1"] = "2"
            });
            
            var expected = new OutgoingMessage("topic", new byte[] {1, 2, 3}, new Dictionary<string, string>()
            {
                ["header1"] = "2"
            });
            actual.ShouldBe(expected);
        }
    }
}