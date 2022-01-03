using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Goncolos.Consumers;
using Goncolos.Infra;
using Goncolos.Producers;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Goncolos.HighLevel.Serializations
{
    public class JsonMessageSerializer
        : IMessageSerializer
    {
        public JsonSerializer Serializer { get; set; } = new JsonSerializer
        {
            DateTimeZoneHandling = DateTimeZoneHandling.Utc
        };

        public async ValueTask<object> Deserialize(IncomingMessage incomingMessage)
        {
            var jObject = await ParseAsJson(incomingMessage);
            if (jObject == null)
            {
                return null;
            }

            var type = GetMessageType(incomingMessage, jObject);
            if (type == null)
            {
                return null;
            }

            var message = BuildMessageObject(incomingMessage, jObject, type);
            return message is JToken ? null : message;
        }

        private string SerializeObject(object value)
        {
            var sb = new StringBuilder(256);
            var sw = new StringWriter(sb, CultureInfo.InvariantCulture);
            using var jsonWriter = new JsonTextWriter(sw) { Formatting = Formatting.None };
            Serializer.Serialize(jsonWriter, value, null);
            return sw.ToString();
        }

        public ValueTask<OutgoingMessage> Serialize(object message, TopicWithPartition topic, Dictionary<string, string> headers = null)
        {
            var json = SerializeObject(message);
            var body = Constants.HeaderEncoding.GetBytes(json);
            var messageId = Headers.CreateMessageId();
            var outgoingMessage = new OutgoingMessage(topic, body, headers)
                .SetHeaderIfNotExists(Headers.MessageId, () => messageId)
                .SetHeaderIfNotExists(Headers.CorrelationId, () => messageId)
                .SetHeader(Headers.PublishedAt, SystemTime.UtcNowOffset)
                .SetHeader(Headers.Host,Constants.Host)
                .SetHeader(Headers.GoncolosVersion,Constants.AssemblyVersion)
                .SetHeader(Headers.MessageType, message.GetType().AssemblyQualifiedName);
            return new ValueTask<OutgoingMessage>(outgoingMessage);
        }

        protected virtual ValueTask<JObject> ParseAsJson(IncomingMessage incomingMessage)
        {
            try
            {
                var json = Constants.HeaderEncoding.GetString(incomingMessage.Body.Span);
                var jObject=  JObject.Parse(json);
                return new ValueTask<JObject>(jObject);
            }
            catch (Exception e)
            {
                throw new KafkaSerializationException($"error occurred deserializing body as json, body: {incomingMessage.Body}, message dropped!", e);
            }
        }

        protected virtual object BuildMessageObject(IncomingMessage incomingMessage, JObject parsedMessage, Type type)
        {
            try
            {
                var message = parsedMessage.ToObject(type, Serializer);
                if (message == null)
                {
                    throw new KafkaSerializationException($"message could not be bind to object, body={parsedMessage}, type={type.AssemblyQualifiedName}");
                }

                return message;
            }
            catch (Exception e)
            {
                throw new KafkaSerializationException($"error occurred binding message to object, body={parsedMessage}, type={type.AssemblyQualifiedName}", e);
            }
        }

        protected virtual Type GetMessageType(IncomingMessage incomingMessage, JObject parsedMessage)
        {
            var typeName = incomingMessage.Headers.GetValueOrDefault("x-type", null)
                           ?? parsedMessage.GetValue("type", StringComparison.InvariantCultureIgnoreCase)?.ToString()
                           ?? parsedMessage.GetValue("x-type", StringComparison.InvariantCultureIgnoreCase)?.ToString();

            if (string.IsNullOrEmpty(typeName))
            {
                throw new KafkaSerializationException($"Missing type property in message, message={parsedMessage.ToString(Formatting.None)}");
            }

            var type = Type.GetType(typeName);
            if (type == null)
            {
                throw new KafkaSerializationException($"Type not found in assembly or mapping, message={parsedMessage.ToString(Formatting.None)}, type={typeName}");
            }

            return type;
        }
    }
}