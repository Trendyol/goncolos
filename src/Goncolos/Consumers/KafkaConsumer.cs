using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Goncolos.Consumers.Configuration;
using Microsoft.Extensions.Logging;

namespace Goncolos.Consumers
{
    public class KafkaConsumer : IKafkaConsumer
    {
        private readonly ConcurrentDictionary<TopicPartition, ITopicPartitionWorker> _workers
            = new ConcurrentDictionary<TopicPartition, ITopicPartitionWorker>();
        private readonly KafkaConsumerConfiguration _configuration;
        private readonly IConsumer<string, byte[]> _innerConsumer;
        private readonly IDisposable _kafkaLoggerScope;
        private readonly object _lock = new object();
        private CancellationTokenSource _loopCancellation;
        private readonly ConcurrentQueue<ValueTask> _revokedWorkers = new ConcurrentQueue<ValueTask>();
        private readonly ITopicStateManager _stateManager;

        public string Name => _innerConsumer.Name;

        public KafkaConsumer(KafkaConsumerConfiguration configuration)
        {
            _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
            var builder = configuration.CreateConsumerBuilder();
            _innerConsumer = builder
                .SetPartitionsAssignedHandler(PartitionsAssignedHandler)
                .SetPartitionsRevokedHandler(PartitionsRevokedHandler)
                .Build();

            var state = new Dictionary<string, object>
            {
                [Constants.ConsumerLoggerFields.ClientId] = _configuration.ClientId,
                [Constants.ConsumerLoggerFields.ConsumerName] = _innerConsumer.Name,
                [Constants.ConsumerLoggerFields.Group] = _configuration.GroupId
            };
            _kafkaLoggerScope = _configuration.Logger.BeginScope(state);
            _stateManager = new TopicStateManager(_innerConsumer, configuration.Logger);
        }

        public async ValueTask DisposeAsync()
        {
            if (_loopCancellation == null || _loopCancellation.IsCancellationRequested)
            {
                return;
            }

            _loopCancellation.Cancel();
            _configuration.Logger.LogInformation($"{Name} kafka consumer closing... ");
            foreach (var (topicPartition, result) in _workers)
            {
                await result.DisposeAsync();
            }

            while (_revokedWorkers.TryDequeue(out var revokeTask))
            {
                await revokeTask;
            }

            _innerConsumer.Unsubscribe();
            _workers.Clear();
            _innerConsumer.Close();
            _innerConsumer.Dispose();
            _configuration.Logger.LogInformation($"{Name} kafka consumer closed.");
            _loopCancellation.Dispose();

            try
            {
                if (_configuration.ConsumerDisposedHandler != null)
                {
                    await _configuration.ConsumerDisposedHandler();
                }

                _kafkaLoggerScope?.Dispose();
            }
            catch
            {
                // ignore
            }
        }

        public async Task Start(CancellationToken cancellationToken = default)
        {
            await Task.Yield();
            var topics = string.Join(",", _configuration.Topics);
            try
            {
                _innerConsumer.Subscribe(_configuration.Topics);
                _loopCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                _ = Task.Run(Poll, _loopCancellation.Token);
                _configuration.Logger.LogInformation($"Kafka consumer started, subscribed to topics: {topics}");
            }
            catch (Exception e)
            {
                _configuration.Logger.LogError(e, $"An error occurred when subscribing kafka topics: {topics}");
                throw;
            }
        }

        private Dictionary<string, object> GetExceptionParameter(Exception ex)
        {
            if (ex is KafkaException kafkaEx)
            {
                var error = kafkaEx.Error;
                return new Dictionary<string, object>
                {
                    [nameof(error.Reason)] = error.ToString(),
                    [nameof(error.IsError)] = error.IsError,
                    [nameof(error.IsFatal)] = error.IsFatal,
                    [nameof(error.IsBrokerError)] = error.IsBrokerError,
                    [nameof(error.IsLocalError)] = error.IsLocalError,
                };
            }

            return new Dictionary<string, object>();
        }

        private bool IsDropException(KafkaException ex)
        {
            return ex.Error.IsFatal || ex.Error.Code == ErrorCode.UnknownTopicOrPart;
        }

        private void PartitionsAssignedHandler(IConsumer<string, byte[]> consumer, List<TopicPartition> assignedTopicPartitions)
        {
            lock (_lock)
            {
                if (_loopCancellation.IsCancellationRequested)
                {
                    return;
                }

                if (assignedTopicPartitions.Any())
                {
                    _configuration.Logger.LogInformation($"Partitions assigned: ({string.Join(",", assignedTopicPartitions)})");
                }

                foreach (var topicPartition in assignedTopicPartitions)
                {
                    var topicWithPartition = topicPartition;
                    if (_workers.ContainsKey(topicPartition))
                    {
                        continue;
                    }

                    if (!_workers.TryAdd(topicWithPartition, Factory(topicPartition)))
                    {
                        _configuration.Logger.LogWarning($"cannot assign same topic partition multiple times: {topicPartition}");
                    }
                }

                foreach (var topicPartition in _workers.Keys.ToArray())
                {
                    if (assignedTopicPartitions.Contains(topicPartition))
                    {
                        continue;
                    }

                    if (_workers.TryRemove(topicPartition, out var worker))
                    {
                        _revokedWorkers.Enqueue(worker.DisposeAsync());
                    }
                }
            }

            ITopicPartitionWorker Factory(TopicPartition topicPartition)
            {
                return new ChannelBasedWorker(_stateManager, topicPartition, _configuration);
            }
        }

        private void PartitionsRevokedHandler(IConsumer<string, byte[]> consumer, List<TopicPartitionOffset> revokedPartitions)
        {
            lock (_lock)
            {
                foreach (var topicPartitionOffset in revokedPartitions)
                {
                    if (_workers.TryGetValue(topicPartitionOffset.TopicPartition, out var worker))
                    {
                        _stateManager.CommitLatestStoredOffset(topicPartitionOffset.TopicPartition);
                    }
                }

                if (revokedPartitions.Any())
                {
                    _configuration.Logger.LogInformation($"Partitions revoked: ({string.Join(",", revokedPartitions)})");
                }
            }
        }

        private async Task Poll()
        {
            while (!_loopCancellation.Token.IsCancellationRequested)
            {
                Exception dropException = null;
                try
                {
                    var consumeResult = _innerConsumer.Consume(_configuration.MaxDelayForPolling);
                    if (consumeResult == null || _loopCancellation.IsCancellationRequested)
                    {
                        continue;
                    }

                    if (consumeResult.IsPartitionEOF)
                    {
                        _configuration.Logger.LogTrace($"Reached end of topic {consumeResult.TopicPartitionOffset}");
                        continue;
                    }

                    if (_workers.TryGetValue(consumeResult.TopicPartition, out var queue))
                    {
                        try
                        {
                            await queue.Enqueue(consumeResult);
                        }
                        catch (TaskCanceledException)
                        {
                            _configuration.Logger.LogWarning($"message enqueue failed, worker cancelled, {consumeResult.TopicPartitionOffset}");
                        }
                        catch (ObjectDisposedException)
                        {
                            _configuration.Logger.LogWarning($"message enqueue failed, worker disposed, {consumeResult.TopicPartitionOffset}");
                        }
                    }
                    else
                    {
                        _configuration.Logger.LogWarning($"Not found partition assignment to process message, {consumeResult.TopicPartitionOffset}");
                    }
                }
                catch (OperationCanceledException oce) when (oce.CancellationToken == _loopCancellation.Token)
                {
                    break;
                }
                catch (ObjectDisposedException) when (_loopCancellation.IsCancellationRequested)
                {
                    break;
                }
                catch (KafkaException ex) when (!IsDropException(ex))
                {
                    _configuration.Logger.LogWarning(ex, "A transient client error occurred on kafka consumer, retrying. (details={details})", GetExceptionParameter(ex));
                    continue;
                }
                catch (Exception e)
                {
                    dropException = e;
                }

                if (dropException == null)
                {
                    continue;
                }

                var exceptionParameters = GetExceptionParameter(dropException);
                _configuration.Logger.LogWarning(dropException, "Unhandled kafka error occurred, handling... (details={details})", exceptionParameters);
                try
                {
                    var behaviour = await _configuration.ConsumerDroppedHandler(this, dropException);
                    if (behaviour == RecoveryBehaviour.Stop)
                    {
                        _configuration.Logger.LogInformation("Unhandled kafka error handled, stopping kafka consumer!");
                        break;
                    }

                    if (behaviour == RecoveryBehaviour.Retry)
                    {
                        _configuration.Logger.LogInformation("Unhandled kafka error handled,retrying.");
                    }
                }
                catch (Exception e)
                {
                    _configuration.Logger.LogError(e, "Unhandled kafka error occurred, stopping kafka consumer!");
                    break;
                }
            }

            await DisposeAsync();
        }
    }
}