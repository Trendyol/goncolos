using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Confluent.Kafka;
using Goncolos.Consumers.Configuration;
using Microsoft.Extensions.Logging;
using Open.ChannelExtensions;

namespace Goncolos.Consumers
{
    public class ChannelBasedWorker : ITopicPartitionWorker
    {
        private readonly TopicPartition _assignedTopicPartition;
        private readonly Channel<ConsumeResult<string, byte[]>> _channel;
        private readonly KafkaConsumerConfiguration _configuration;
        private readonly int _maxRetryToQueue = 3;
        private readonly SubscriptionOptions _subscriptionOptions;
        private readonly CancellationTokenSource _workerCts;
        private readonly ITopicStateManager _topicStateManager;
        private Offset _latestQueuedOffset = Offset.Unset;
        private readonly ValueTask<long> _batchProcessTask;
        private static readonly Random _randomizer = new Random();

        public ChannelBasedWorker(ITopicStateManager topicStateManager, TopicPartition assignedTopicPartition, KafkaConsumerConfiguration configuration)
        {
            _assignedTopicPartition = assignedTopicPartition ?? throw new ArgumentNullException(nameof(assignedTopicPartition));
            _topicStateManager = topicStateManager;
            _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
            _workerCts = new CancellationTokenSource();
            _subscriptionOptions = _configuration.GetTopicOptions(assignedTopicPartition);
            _channel = Channel.CreateBounded<ConsumeResult<string, byte[]>>(
                new BoundedChannelOptions(_subscriptionOptions.BatchOptions.InputQueueMaxSize)
                {
                    SingleWriter = true,
                    SingleReader = true,
                    FullMode = BoundedChannelFullMode.Wait
                });
            _batchProcessTask = _channel
                .Reader
                .Batch(_subscriptionOptions.BatchOptions.BatchSize)
                .WithTimeout(_subscriptionOptions.BatchOptions.BatchTimeout)
                .ReadAllAsync(ProcessBatch, _workerCts.Token);
            _topicStateManager.Assign(assignedTopicPartition);
            _configuration.Logger.LogInformation($"worker starting, ({_assignedTopicPartition})");
        }

        public async ValueTask DisposeAsync()
        {
            if (_workerCts.IsCancellationRequested)
            {
                return;
            }

            _configuration.Logger.LogInformation($"worker closing, ({_assignedTopicPartition})");
            _channel.Writer.Complete();
            var timeoutTask = Task.Delay(_subscriptionOptions.MaxTimeoutToStop);
            var finalizeTask = await Task.WhenAny(_batchProcessTask.AsTask(), timeoutTask);
            _workerCts.Cancel();
            _workerCts.Dispose();
            _topicStateManager.CommitLatestStoredOffset(_assignedTopicPartition);
            _topicStateManager.Resume(_assignedTopicPartition);
            _topicStateManager.Revoke(_assignedTopicPartition);
            _configuration.Logger.LogInformation($"worker closed with '{(finalizeTask == timeoutTask ? "timeout" : "completion")}',  ({_assignedTopicPartition})");
        }

        public async ValueTask Enqueue(ConsumeResult<string, byte[]> result)
        {
            if (_workerCts.IsCancellationRequested)
            {
                await Task.FromCanceled(_workerCts.Token);
                return;
            }

            if (result.Offset <= _topicStateManager.GetLatestCommittedOffset(_assignedTopicPartition))
            {
                return;
            }

            if (result.Offset <= _latestQueuedOffset)
            {
                return;
            }

            var queued = false;
            for (var i = 0; i < _maxRetryToQueue; i++)
            {
                queued = _channel.Writer.TryWrite(result);
                if (queued)
                {
                    break;
                }

                await Task.Delay(10, _workerCts.Token);
            }

            if (queued)
            {
                _latestQueuedOffset = result.Offset;
            }
            else
            {
                _topicStateManager.Pause(result.TopicPartitionOffset);
            }
        }

        private async ValueTask ProcessBatch(List<ConsumeResult<string, byte[]>> messages)
        {
            var retryAttempt = 0;
            while (true)
            {
                using var tokenSource = CancellationTokenSource.CreateLinkedTokenSource(_workerCts.Token);
                var cancellationToken = tokenSource.Token;
                try
                {
                    tokenSource.Token.ThrowIfCancellationRequested();

                    var lastMessage = messages.Last();
                    var lastOffset = lastMessage.Offset;
                    var latestCommittedOffset = _topicStateManager.GetLatestCommittedOffset(lastMessage.TopicPartition);
                    if (lastOffset < latestCommittedOffset)
                    {
                        _configuration.Logger.LogWarning($"offset problem detected, ignored messages. (processed latest offset={latestCommittedOffset}, current offset={lastOffset}, topic={_assignedTopicPartition})");
                        break;
                    }

                    var incomingMessages = messages
                        .Select(m => _subscriptionOptions.IncomingMessageConverter.Convert(m))
                        .ToArray();
                    await _subscriptionOptions.MessageReceivedHandler(incomingMessages, cancellationToken);

                    _topicStateManager.StoreOffset(lastMessage.TopicPartitionOffset);
                    _topicStateManager.Resume(lastMessage.TopicPartition);
                    break;
                }
                catch (OperationCanceledException oce) when (oce.CancellationToken == cancellationToken)
                {
                    break;
                }
                catch (ObjectDisposedException ode) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception e)
                {
                    retryAttempt++;
                    var duration = TimeSpan.FromMilliseconds(Math.Pow(10, retryAttempt)) + TimeSpan.FromMilliseconds(_randomizer.Next(3, 30));
                    _configuration.Logger.LogError(e, $"Unhandled exception occurred processing messages, retrying after {duration} seconds... (retry attempt={retryAttempt}, topic={_assignedTopicPartition})");
                }
            }
        }

        protected bool Equals(ChannelBasedWorker other)
        {
            return Equals(_assignedTopicPartition, other._assignedTopicPartition);
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

            if (obj.GetType() != GetType())
            {
                return false;
            }

            return Equals((ChannelBasedWorker)obj);
        }

        public override int GetHashCode()
        {
            return _assignedTopicPartition != null ? _assignedTopicPartition.GetHashCode() : 0;
        }

        public static bool operator ==(ChannelBasedWorker left, ChannelBasedWorker right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(ChannelBasedWorker left, ChannelBasedWorker right)
        {
            return !Equals(left, right);
        }
    }
}