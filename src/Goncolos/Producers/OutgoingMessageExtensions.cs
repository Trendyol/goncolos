using System;
using System.Diagnostics;

namespace Goncolos.Producers
{
    public static class OutgoingMessageExtensions
    {
        public static OutgoingMessage SetHeader(this OutgoingMessage outgoingMessage, string key, DateTimeOffset value)
        {
            outgoingMessage.Headers[key] = value.ToString("O");
            return outgoingMessage;
        }
        
        public static OutgoingMessage SetHeader(this OutgoingMessage outgoingMessage, string key, Exception value)
        {
            outgoingMessage.Headers[key] = value.ToStringDemystified();
            return outgoingMessage;
        }
        
        public static OutgoingMessage UpdateHeaderOrDefault(this OutgoingMessage outgoingMessage, string key,Func<string,string> updateFunc, Func<string> valueFactory)
        {
            if (outgoingMessage.Headers.TryGetValue(key, out var v))
            {
                v = updateFunc(v);
            }
            else
            {
                v = valueFactory();
            }

            outgoingMessage.Headers[key] = v;
            return outgoingMessage;
        }
        
        public static OutgoingMessage SetHeaderIfNotExists(this OutgoingMessage outgoingMessage, string key, Func<string> valueFactory)
        {
            return outgoingMessage.Headers.ContainsKey(key) ? outgoingMessage : outgoingMessage.SetHeader(key, valueFactory());
        }
    }
}