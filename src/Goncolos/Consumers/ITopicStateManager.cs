using Confluent.Kafka;

namespace Goncolos.Consumers
{
    public interface ITopicStateManager
    {
        void Resume(TopicPartition topicPartition);
        void Revoke(TopicPartition topicPartition);
        void Assign(TopicPartition topicPartition);
        void Pause(TopicPartitionOffset topicPartitionOffset);
        Offset GetLatestCommittedOffset(TopicPartition topicPartition);
        void StoreOffset(TopicPartitionOffset topicPartitionOffset);
        void CommitLatestStoredOffset(TopicPartition topicPartition);
    }
}