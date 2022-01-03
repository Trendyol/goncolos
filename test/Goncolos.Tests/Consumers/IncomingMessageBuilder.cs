using System;
using System.Collections.Generic;
using Goncolos.Consumers;

namespace Goncolos.Tests.Consumers
{
    public class IncomingMessageBuilder
    {
        public string Key { get; set; }
        public byte[] Body { get;set; }
        public DateTimeOffset Timestamp { get;set; }
        public Dictionary<string, string> Headers { get;set; }
        public TopicWithPartition Topic { get;set; }
        public long Offset { get; set;}

        public IncomingMessageBuilder()
        {
            
        }

        public IncomingMessage Build()
        {
            return new IncomingMessage(Topic, Offset, Headers, Body, Key, Timestamp);
        }
    }
}