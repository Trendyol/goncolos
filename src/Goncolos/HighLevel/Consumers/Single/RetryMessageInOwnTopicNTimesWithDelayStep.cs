using System;
using System.Threading;
using System.Threading.Tasks;
using Goncolos.Consumers;
using Goncolos.Infra;
using Goncolos.Infra.Pipeline;
using Goncolos.Producers;
using Microsoft.Extensions.Logging;

namespace Goncolos.HighLevel.Consumers.Single
{
    public class RetryMessageInOwnTopicNTimesWithDelayStepOptions
    {
        public RetryMessageInOwnTopicNTimesWithDelayStepOptions(TimeSpan delay, int retryCount = 3)
        {
            RetryCount = retryCount;
            Delay = delay;
        }

        public int RetryCount { get; set; }
        public TimeSpan Delay { get; set; }
    }

    public class RetryMessageInOwnTopicNTimesWithDelayStep
        : IPipelineStep<SingleIncomingMessageContext>
    {
        private readonly IKafkaProducer _kafkaProducer;
        private readonly ILogger _logger;
        private readonly RetryMessageInOwnTopicNTimesWithDelayStepOptions _options;

        public RetryMessageInOwnTopicNTimesWithDelayStep(ILogger logger, IKafkaProducer kafkaProducer, RetryMessageInOwnTopicNTimesWithDelayStepOptions options)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _kafkaProducer = kafkaProducer ?? throw new ArgumentNullException(nameof(kafkaProducer));
            _options = options ?? throw new ArgumentNullException(nameof(options));
        }

        public async Task Execute(SingleIncomingMessageContext context, PipelineStepDelegate<SingleIncomingMessageContext> next)
        {
            try
            {
                await WaitIfNecessary(context.IncomingMessage, context.CancellationToken);
                await next(context);
            }
            catch (OperationCanceledException oce) when (oce.CancellationToken == context.CancellationToken) // consumer closing
            {
                throw;
            }
            catch (Exception e)
            {
                if (!CanRetry(context.IncomingMessage, out var totalRetryCount))
                {
                    throw;
                }

                _logger.LogWarning(e, $"an error occurred processing message, sending failed message to own topic to retry, incoming message={context.IncomingMessage}, total retry={totalRetryCount}, retry topic={{topic}}", context.IncomingMessage.Topic);
                try
                {
                    await SendToOwnTopicWithDelay(context, e);
                }
                catch (Exception exception)
                {
                    _logger.LogError(exception, $"error occurred sending failed message to retry topic, incoming message={context.IncomingMessage}, total retry={totalRetryCount}, retry topic={{retryTopic}}", context.IncomingMessage.Topic);
                    throw;
                }
            }
        }

        public virtual bool CanRetry(IncomingMessage incomingMessage, out int totalRetryCount)
        {
            totalRetryCount = GetTotalRetryCount(incomingMessage);
            return totalRetryCount < _options.RetryCount;
        }

        public TimeSpan GetDelay(DateTimeOffset publishedAt)
        {
            var now = SystemTime.UtcNowOffset;
            var delay = _options.Delay;
            if (publishedAt > now) // when consumer's clock is behind from publisher clock 
            {
                return delay;
            }

            var delayTo = publishedAt + delay;
            var diffToDelay = delayTo - now;
            return diffToDelay;
        }

        private int GetTotalRetryCount(IncomingMessage incomingMessage)
        {
            if (!incomingMessage.Headers.TryGetValue(Headers.RetryCount, out var v) || !int.TryParse(v, out var retryCount))
            {
                return 0;
            }

            return retryCount;
        }

        private async ValueTask WaitIfNecessary(IncomingMessage incomingMessage, CancellationToken cancellationToken)
        {
            if (_options.Delay <= TimeSpan.Zero)
            {
                return;
            }

            if (!TryGetPublishedDate(incomingMessage, out var publishedAt))
            {
                return;
            }

            var delay = GetDelay(publishedAt);
            if (delay <= TimeSpan.Zero)
            {
                return;
            }

            await Task.Delay(delay, cancellationToken);
            _logger.LogTrace($"message delayed: {delay}, message detail={incomingMessage}");
        }

        private static bool TryGetPublishedDate(IncomingMessage incomingMessage, out DateTimeOffset dt)
        {
            dt = default;
            return incomingMessage.Headers.TryGetValue(Headers.PublishedAt, out var v) && DateTimeOffset.TryParse(v, out dt);
        }

        private async Task SendToOwnTopicWithDelay(SingleIncomingMessageContext context, Exception e)
        {
            var incomingMessage = context.IncomingMessage;
            var now = SystemTime.UtcNowOffset;
            var outgoingMessage = new OutgoingMessage(incomingMessage.Topic, incomingMessage.Body, incomingMessage.Headers)
                .WithKey(incomingMessage.Key)
                .SetHeader(Headers.PublishedAt, now)
                .SetHeader(Headers.Exception, e)
                .SetHeader(Headers.ExceptionAt, now)
                .SetHeaderIfNotExists(Headers.OriginalTopic, () => incomingMessage.Topic.ToString())
                .UpdateHeaderOrDefault(Headers.RetryCount, v => (int.Parse(v) + 1).ToString(), () => "1");
            await _kafkaProducer.Produce(outgoingMessage, context.CancellationToken);
        }
    }
}