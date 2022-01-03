using System;

namespace Goncolos.HighLevel.Serializations
{
    public class KafkaSerializationException : Exception
    {
        public KafkaSerializationException(string message) : base(message)
        {
        }

        public KafkaSerializationException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}