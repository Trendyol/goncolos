using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace Goncolos.Producers
{
    public class KafkaProducer
        : IKafkaProducer
    {
      private readonly KafkaProducerConfiguration _configuration;
        private readonly IProducer<string, byte[]> _producer;

        public KafkaProducer(KafkaProducerConfiguration configuration)
        {
            _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
            var builder = configuration.CreateProducerBuilder();
            _producer = builder.Build();
        }

        public Task ProduceBulkWithCallback(OutgoingMessage[] messages)
        {
            var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            long messageLength = messages.Length;

            void DeliveryHandler(DeliveryReport<string, byte[]> deliveryReport)
            {
                if (deliveryReport.Error.IsError)
                {
                    _configuration.Logger.LogWarning($"An error occurred producing message: {deliveryReport.Error.Reason}");
                    if (_configuration.ShouldInterrupt(deliveryReport.Error))
                    {
                        tcs.SetException(new KafkaProducerException(deliveryReport.Error.ToString()));
                        return;
                    }
                }

                var v = Interlocked.Decrement(ref messageLength);
                if (v == 0)
                {
                    tcs.SetResult(true);
                }
            }

            for (var i = 0; i < messages.Length; i += 1)
            {
                var message = messages[i];
                try
                {
                    var headers = CreateHeaders(message);
                    _producer.Produce(message.Topic.ToTopicPartition(),  new Message<string, byte[]>
                    {
                        Value = message.Body.ToArray(),
                        Headers = headers,
                        Key = message.Key
                    }, DeliveryHandler);
                }
                catch (ProduceException<Null, byte[]> ex)
                {
                    if (ex.Error.Code == ErrorCode.Local_QueueFull)
                    {
                        _producer.Poll(TimeSpan.FromMilliseconds(200));
                        i -= 1;
                    }
                    else
                    {
                        tcs.SetException(ex);
                        break;
                    }
                }
            }

            return tcs.Task;
        }

        public async Task ProduceBulk(OutgoingMessage[] messages, CancellationToken cancellationToken = default)
        {
            var tasks = new List<Task>(messages.Length);
            foreach (var message in messages)
            {
                var headers = CreateHeaders(message);
                var task = _producer.ProduceAsync(message.Topic.ToTopicPartition(), new Message<string, byte[]>
                {
                    Value = message.Body.ToArray(),
                    Headers = headers,
                    Key = message.Key
                }, cancellationToken);
                tasks.Add(task);
            }

            await Task.WhenAll(tasks);
        }

        public async ValueTask Produce(OutgoingMessage message, CancellationToken cancellationToken)
        {
            var headers = CreateHeaders(message);
            var topicPartition = message.Topic.ToTopicPartition();
            await _producer.ProduceAsync(topicPartition, new Message<string, byte[]>
            {
                Value = message.Body.Span.ToArray(),
                Headers = headers,
                Key = message.Key
            }, cancellationToken);
        }

        public void Dispose()
        {
            try
            {
                _producer.Flush(TimeSpan.FromSeconds(5));
            }
            catch (Exception e)
            {
                _configuration.Logger.LogDebug(e, "An error occurred flushing producer.");
            }

            _producer.Dispose();
        }

        private static Headers CreateHeaders(OutgoingMessage message)
        {
            var headers = new Headers();
            foreach (var (key, value) in message.Headers)
            {
                headers.Add(key, Constants.HeaderEncoding.GetBytes(value));
            }

            return headers;
        }
    }
}