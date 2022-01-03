using System;
using Confluent.Kafka;

namespace Goncolos
{
    public class TopicWithPartition
    {
        public const int AnyPartition = -1;
        public string Topic { get; }
        public int Partition { get; }

        public TopicWithPartition(string topic)
        {
            Topic = topic;
            Partition = AnyPartition;
        }

        public TopicWithPartition(string topic, int partition)
        {
            Topic = topic;
            Partition = Math.Abs(partition);
        }

        protected bool Equals(TopicWithPartition other)
        {
            return Topic == other.Topic && Partition == other.Partition;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
            {
                return false;
            }

            if (ReferenceEquals(this, obj))
            {
                return true;
            }

            if (obj.GetType() != this.GetType())
            {
                return false;
            }

            return Equals((TopicWithPartition) obj);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Topic, Partition);
        }

        public static bool operator ==(TopicWithPartition left, TopicWithPartition right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(TopicWithPartition left, TopicWithPartition right)
        {
            return !Equals(left, right);
        }

        internal TopicPartition ToTopicPartition()
        {
            return new TopicPartition(Topic, new Partition(Partition));
        }

        public static implicit operator TopicPartition(TopicWithPartition topicWithPartition)
        {
            return topicWithPartition.ToTopicPartition();
        }
        public static implicit operator TopicWithPartition(TopicPartition topicWithPartition)
        {
            return new TopicWithPartition(topicWithPartition.Topic, topicWithPartition.Partition);
        }
        
        public static implicit operator TopicWithPartition(string partition)
        {
            return new TopicWithPartition(partition);
        }

        public TopicWithPartition WithPartition(in int partition)
        {
            return new TopicWithPartition(Topic, partition);
        }
        
        public override string ToString() => $"{Topic} [{Partition}]";
    }
}