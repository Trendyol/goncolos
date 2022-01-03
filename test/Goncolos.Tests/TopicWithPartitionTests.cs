using Shouldly;
using Xunit;

namespace Goncolos.Tests
{
    public class TopicWithPartitionTests
    {
        [Fact]
        public void should_convert_readable_string()
        {
            var actual = new TopicWithPartition("topic1",3);
            actual.ToString().ShouldBe("topic1 [3]");
        }
        
        [Fact]
        public void should_convert_implicitly_from_string()
        {
            TopicWithPartition actual = "topic1";
            actual.ShouldBe(new TopicWithPartition("topic1"));
        }
        
        [Fact]
        public void should_use_any_partition_when_partition_not_specified()
        {
            var actual = new TopicWithPartition("topic1");
            actual.Partition.ShouldBe(-1);
        }
        
        [Fact]
        public void should_equal_with_same_topic()
        {
            var actual = new TopicWithPartition("topic1");
            var expected = new TopicWithPartition("topic1");
            actual.ShouldBe(expected);
        }
        
        [Fact]
        public void should_equal_with_same_topic_and_same_partition()
        {
            var actual = new TopicWithPartition("topic1",3);
            var expected = new TopicWithPartition("topic1",3);
            actual.ShouldBe(expected);
        }
        
        [Fact]
        public void should_change_topic_partition()
        {
            var actual = new TopicWithPartition("topic1", 5).WithPartition(3);
            var expected = new TopicWithPartition("topic", 3);
            actual.ShouldNotBe(expected);
        }
        
        [Fact]
        public void should_not_equal_with_same_topic_and_different_partition()
        {
            var actual = new TopicWithPartition("topic1",5);
            var expected = new TopicWithPartition("topic1",3);
            actual.ShouldNotBe(expected);
        }
    }
}