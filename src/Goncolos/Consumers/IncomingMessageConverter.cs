using System;
using System.Collections.Generic;
using Confluent.Kafka;

namespace Goncolos.Consumers
{
    public class IncomingMessageConverter
    {
        public IncomingMessage Convert(ConsumeResult<string, byte[]> consumeResult)
        {
            var incomingMessage = new IncomingMessage(
                consumeResult.TopicPartition,
                consumeResult.Offset,
                GetHeaders(consumeResult),
                consumeResult.Message.Value,
                consumeResult.Message.Key,
                DateTimeOffset.FromUnixTimeMilliseconds(consumeResult.Message.Timestamp.UnixTimestampMs)
            );

            return incomingMessage;
        }

        private static Dictionary<string, string> GetHeaders(ConsumeResult<string, byte[]> consumeResult)
        {
            var headers = new Dictionary<string, string>(StringComparer.InvariantCultureIgnoreCase);
            if (consumeResult.Message.Headers == null)
            {
                return headers;
            }

            foreach (var header in consumeResult.Message.Headers)
            {
                var bytes = header.GetValueBytes();
                if (bytes == null)
                {
                    continue;
                }

                headers[header.Key] = Constants.HeaderEncoding.GetString(bytes);
            }

            return headers;
        }
    }
}