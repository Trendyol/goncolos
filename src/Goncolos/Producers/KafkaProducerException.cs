using System;

namespace Goncolos.Producers
{
    public class KafkaProducerException : Exception 
    {
        public KafkaProducerException()
        {
        }

        public KafkaProducerException(string message) : base(message)
        {
        }

        public KafkaProducerException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}