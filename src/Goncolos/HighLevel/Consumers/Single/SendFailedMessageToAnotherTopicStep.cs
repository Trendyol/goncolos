using System;
using System.Threading.Tasks;
using Goncolos.Infra;
using Goncolos.Infra.Pipeline;
using Goncolos.Producers;
using Microsoft.Extensions.Logging;

namespace Goncolos.HighLevel.Consumers.Single
{
    public class SendFailedMessageToAnotherTopicStepOptions
    {
        public string TopicName { get; }

        public SendFailedMessageToAnotherTopicStepOptions(string topicName)
        {
            TopicName = topicName ?? throw new ArgumentNullException(nameof(topicName));
        }
    }

    public class SendFailedMessageToAnotherTopicStep
        : IPipelineStep<SingleIncomingMessageContext>
    {
        private readonly ILogger _logger;
        private readonly IKafkaProducer _kafkaProducer;
        private readonly SendFailedMessageToAnotherTopicStepOptions _options;

        public SendFailedMessageToAnotherTopicStep(ILogger logger, IKafkaProducer kafkaProducer, SendFailedMessageToAnotherTopicStepOptions options)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _kafkaProducer = kafkaProducer ?? throw new ArgumentNullException(nameof(kafkaProducer));
            _options = options ?? throw new ArgumentNullException(nameof(options));
        }

        public async Task Execute(SingleIncomingMessageContext context, PipelineStepDelegate<SingleIncomingMessageContext> next)
        {
            try
            {
                await next(context);
            }
            catch (OperationCanceledException oce) when (oce.CancellationToken == context.CancellationToken) // consumer closing
            {
                throw;
            }
            catch (Exception e)
            {
                _logger.LogWarning(e, $"an error occurred processing message, sending failed message to another topic, incoming message={context.IncomingMessage},  topic={{topic}}", _options.TopicName);
                try
                {
                    await SendToAnotherTopic(context, e);
                }
                catch (Exception exception)
                {
                    _logger.LogError(exception, $"error occurred sending failed message to another topic, {context.IncomingMessage}, topic={{topic}}", _options.TopicName);
                    throw;
                }
            }
        }

        private async Task SendToAnotherTopic(SingleIncomingMessageContext context, Exception e)
        {
            var incomingMessage = context.IncomingMessage;
            var now = SystemTime.UtcNowOffset;
            var outgoingMessage = new OutgoingMessage(_options.TopicName, incomingMessage.Body, incomingMessage.Headers)
                .WithKey(incomingMessage.Key)
                .SetHeader(Headers.PublishedAt, now)
                .SetHeader(Headers.Exception, e)
                .SetHeader(Headers.ExceptionAt, now)
                .SetHeaderIfNotExists(Headers.OriginalTopic, () => incomingMessage.Topic.ToString());
            await _kafkaProducer.Produce(outgoingMessage, context.CancellationToken);
        }
    }
}