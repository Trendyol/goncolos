using System;
using System.Text;
using Goncolos.Consumers;

namespace Goncolos.HighLevel
{
    public static class Headers
    {
        private static Func<string> MessageIdFactory = DefaultMessageIdFactory;
        
        public const string PublishedAt = "X-PublishedAt";
        public const string MessageId = "X-MessageId";
        public const string Host = "X-Host";
        public const string GoncolosVersion = "X-GoncolosVersion";
        public const string Exception = "X-Exception";
        public const string ExceptionAt = "X-ExceptionAt";
        public const string OriginalTopic = "X-OriginalTopic";
        public const string RetryCount = "X-RetryCount";
        public const string CorrelationId = "X-CorrelationId";
        public const string MessageType = "X-Type";
        public const string OriginalPublishedAt = "X-OriginalPublishedAt";
        public const string DelaySeconds = "X-DelaySeconds";
        public const string TargetTopic = "X-TargetTopic";
        
        public static string CreateMessageId() => MessageIdFactory();
        
        private static string DefaultMessageIdFactory()
        {
            return $"Goncolos::{Guid.NewGuid():N}";
        }

        public static void SetMessageIdFactory(Func<string> factory)
        {
            MessageIdFactory = factory;
        }

        public static bool TryGetHeaderAsDateTimeOffset(this IncomingMessage incomingMessage, string key, out DateTimeOffset dt)
        {
            dt = default;
            return incomingMessage.Headers.TryGetValue(key, out var v) && DateTimeOffset.TryParse(v, out dt);
        }

        public static bool TryGetHeaderAsInt(this IncomingMessage incomingMessage, string key, out int dt)
        {
            dt = default;
            return incomingMessage.Headers.TryGetValue(key, out var v) && int.TryParse(v, out dt);
        }

        public static string GetHeaderOrDefault(this IncomingMessage incomingMessage, string key, Func<string> defaultValueFactory = null)
        {
            return incomingMessage.Headers.TryGetValue(key, out var v) ? v : defaultValueFactory?.Invoke();
        }
    }
}