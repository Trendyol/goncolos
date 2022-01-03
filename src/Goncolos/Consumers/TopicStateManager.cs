using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using Goncolos.Infra;
using Microsoft.Extensions.Logging;

namespace Goncolos.Consumers
{
    public class TopicStateManager : ITopicStateManager
    {
        private readonly IConsumer<string, byte[]> _consumer;
        private readonly ILogger _logger;
        private readonly ConcurrentDictionary<TopicPartition, TopicPartitionOffset> _pausedTopicPartitions = new ConcurrentDictionary<TopicPartition, TopicPartitionOffset>();
        private readonly ConcurrentDictionary<TopicPartition, TopicPartitionOffset> _latestCommittedOffset = new ConcurrentDictionary<TopicPartition, TopicPartitionOffset>();
        private readonly ConcurrentDictionary<TopicPartition, DateTimeOffset> _assignedTopics = new ConcurrentDictionary<TopicPartition, DateTimeOffset>();

        public TopicStateManager(IConsumer<string, byte[]> consumer, ILogger logger)
        {
            _consumer = consumer;
            _logger = logger;
        }

        public void Resume(TopicPartition topicPartition)
        {
            if (!_pausedTopicPartitions.TryRemove(topicPartition, out var offset))
            {
                return;
            }

            var topicPartitionOffset = new TopicPartitionOffset(topicPartition, offset.Offset);
            try
            {
                _consumer.Seek(topicPartitionOffset);
                _consumer.Resume(new[] { topicPartition });
                _logger.LogTrace($"kafka consumer resuming, ({topicPartitionOffset})");
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, $"kafka consumer not resumed, ({topicPartitionOffset})");
            }
        }

        public void Assign(TopicPartition topicPartition)
        {
            var now = SystemTime.UtcNowOffset;
            _assignedTopics.AddOrUpdate(topicPartition, _ => now, (_, __) => now);
        }

        public void Revoke(TopicPartition topicPartition)
        {
            _pausedTopicPartitions.TryRemove(topicPartition, out _);
            _latestCommittedOffset.TryRemove(topicPartition, out _);
            _assignedTopics.TryRemove(topicPartition, out _);
        }

        public void Pause(TopicPartitionOffset topicPartitionOffset)
        {
            var topicPartition = topicPartitionOffset.TopicPartition;
            if (_pausedTopicPartitions.TryGetValue(topicPartition, out var _))
            {
                return;
            }

            try
            {
                _consumer.Pause(new[] { topicPartition });
                _pausedTopicPartitions.TryAdd(topicPartition, topicPartitionOffset);
                _logger.LogTrace($"kafka consumer paused, ({topicPartitionOffset})");
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, $"kafka consumer not paused, ({topicPartitionOffset})");
            }
        }

        public Offset GetLatestCommittedOffset(TopicPartition topicPartition)
        {
            return _latestCommittedOffset.TryGetValue(topicPartition, out var latestCommittedOffset) ? latestCommittedOffset.Offset : Offset.Unset;
        }

        public void StoreOffset(TopicPartitionOffset topicPartitionOffset)
        {
            var committingTopicPartitionOffset = new TopicPartitionOffset(topicPartitionOffset.TopicPartition, topicPartitionOffset.Offset + 1);
            var topicPartition = committingTopicPartitionOffset.TopicPartition;
            if (_latestCommittedOffset.TryGetValue(topicPartition, out var latestCommittedOffset) && latestCommittedOffset.Offset > committingTopicPartitionOffset.Offset)
            {
                _logger.LogWarning($"latest committed offset greater than current committing offset, (offset={committingTopicPartitionOffset}, latest committed offset: {latestCommittedOffset})");
            }
            else
            {
                _consumer.StoreOffset(committingTopicPartitionOffset);
                _logger.LogTrace($"offset stored, ({committingTopicPartitionOffset})");
            }

            _latestCommittedOffset[topicPartition] = committingTopicPartitionOffset;
        }

        public void CommitLatestStoredOffset(TopicPartition topicPartition)
        {
            var offset = GetLatestCommittedOffset(topicPartition);
            if (offset == Offset.Unset)
            {
                return;
            }

            CommitOffset(new TopicPartitionOffset(topicPartition, offset));
        }

        private void CommitOffset(TopicPartitionOffset topicPartitionOffset)
        {
            var committingTopicPartitionOffset = new TopicPartitionOffset(topicPartitionOffset.TopicPartition, topicPartitionOffset.Offset);
            var topicPartition = committingTopicPartitionOffset.TopicPartition;
            if (_latestCommittedOffset.TryGetValue(topicPartition, out var latestCommittedOffset) && latestCommittedOffset.Offset > committingTopicPartitionOffset.Offset)
            {
                _logger.LogWarning($"latest committed offset greater than current committing offset, (offset={committingTopicPartitionOffset}, latest committed offset: {latestCommittedOffset})");
            }
            else
            {
                Commit(new[] { committingTopicPartitionOffset });
                _latestCommittedOffset[topicPartition] = committingTopicPartitionOffset;
            }
        }

        private void Commit(IEnumerable<TopicPartitionOffset> topicPartitionOffset)
        {
            var topicOffsets = string.Join(",", topicPartitionOffset.Select(t => t.ToString()));
            try
            {
                _consumer.Commit(topicPartitionOffset);
                _logger.LogTrace($"offsets committed, ({topicOffsets})");
            }
            catch (KafkaException ex) when (ex.Error.Code == ErrorCode.UnknownMemberId)
            {
                // ignore
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, $"an error occurred when committing offset, ({topicOffsets})");
            }
        }
    }
}