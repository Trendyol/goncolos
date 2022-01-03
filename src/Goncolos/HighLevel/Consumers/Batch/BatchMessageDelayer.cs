using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Goncolos.Consumers;
using Goncolos.Infra;
using Goncolos.Infra.Pipeline;
using Goncolos.Producers;
using Microsoft.Extensions.Logging;

namespace Goncolos.HighLevel.Consumers.Batch
{
    public class BatchMessageDelayer
        : IPipelineStep<BatchIncomingMessageContext>
    {
        private readonly IKafkaProducer _producer;
        public string DefaultTargetTopic { get; }

        public BatchMessageDelayer(IKafkaProducer producer,string defaultTargetTopic)
        {
            if (string.IsNullOrEmpty(defaultTargetTopic))
            {
                throw new ArgumentNullException(nameof(defaultTargetTopic));
            }
            _producer = producer ?? throw new ArgumentNullException(nameof(producer));
            DefaultTargetTopic = defaultTargetTopic;
        }

        public async Task Execute(BatchIncomingMessageContext context, PipelineStepDelegate<BatchIncomingMessageContext> next)
        {
            await Process(context);
            await next(context);
        }

        private async Task Process(BatchIncomingMessageContext context)
        {
            var now = SystemTime.UtcNowOffset;
            var messageMap = context.IncomingMessages
                .Select(m => new EnrichedIncomingMessage(m, now, DefaultTargetTopic))
                .ToHashSet();

            await ProcessMessagesNotNeedToBeDelayed(context.CancellationToken, messageMap, now);
            if (!messageMap.Any())
            {
                return;
            }

            await ProcessMessagesToBeDelayed(context.CancellationToken, messageMap);
        }

        private async Task ProcessMessagesToBeDelayed(CancellationToken cancellationToken, HashSet<EnrichedIncomingMessage> messageMap)
        {
            var now = SystemTime.UtcNowOffset;
            var maxDateAfterDelay = messageMap
                .Select(m => m.PublishedAt + m.Delay)
                .Max();
            var delay = maxDateAfterDelay - now;
            if (delay > TimeSpan.Zero)
            {
                await Task.Delay(delay, cancellationToken);
            }

            await ProduceBulk(messageMap, cancellationToken);
        }

        private async Task ProcessMessagesNotNeedToBeDelayed(CancellationToken cancellationToken, HashSet<EnrichedIncomingMessage> messageMap, DateTimeOffset now)
        {
            var notNeedToBeDelayedMessages = messageMap
                .Where(m => now - m.PublishedAt > m.Delay)
                .ToArray();
            await ProduceBulk(notNeedToBeDelayedMessages, cancellationToken);
            messageMap.RemoveWhere(m => notNeedToBeDelayedMessages.Contains(m));
        }

        private Task ProduceBulk(IEnumerable<EnrichedIncomingMessage> incomingMessages, CancellationToken cancellationToken)
        {
            var outgoingMessages = incomingMessages
                .Select(t => new OutgoingMessage(new TopicWithPartition(t.TargetTopic), t.Message.Body, t.Message.Headers)
                    .WithKey(t.Message.Key)
                    .SetHeader(Headers.OriginalPublishedAt, t.PublishedAt)
                    .SetHeader(Headers.OriginalTopic, t.Message.Topic.ToString())
                    .SetHeader(Headers.PublishedAt, SystemTime.UtcNowOffset)
                )
                .ToArray();
            return _producer.ProduceBulk(outgoingMessages, cancellationToken);
        }

        private readonly struct EnrichedIncomingMessage
        {
            public IncomingMessage Message { get; }
            public DateTimeOffset PublishedAt { get; }
            public string TargetTopic { get; }
            public string MessageId { get; }
            public TimeSpan Delay { get; }

            public EnrichedIncomingMessage(IncomingMessage message, DateTimeOffset now, string defaultTargetTopic)
            {
                Message = message;
                MessageId = message.GetHeaderOrDefault(Headers.MessageId, Headers.CreateMessageId);
                TargetTopic = message.Headers.GetValueOrDefault(Headers.TargetTopic, defaultTargetTopic);
                Delay = TimeSpan.FromSeconds(message.TryGetHeaderAsInt(Headers.DelaySeconds, out var delaySeconds) ? delaySeconds : 0);
                PublishedAt = message.TryGetHeaderAsDateTimeOffset(Headers.PublishedAt, out var publishedAt) ? publishedAt : now;
            }

            public bool Equals(EnrichedIncomingMessage other)
            {
                return MessageId == other.MessageId;
            }

            public override bool Equals(object obj)
            {
                return obj is EnrichedIncomingMessage other && Equals(other);
            }

            public override int GetHashCode()
            {
                return MessageId != null ? MessageId.GetHashCode() : 0;
            }

            public static bool operator ==(EnrichedIncomingMessage left, EnrichedIncomingMessage right)
            {
                return left.Equals(right);
            }

            public static bool operator !=(EnrichedIncomingMessage left, EnrichedIncomingMessage right)
            {
                return !left.Equals(right);
            }
        }
    }
}