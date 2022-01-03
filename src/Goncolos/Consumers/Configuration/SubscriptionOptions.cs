using System;
using System.Collections.Generic;

namespace Goncolos.Consumers.Configuration
{
    public class SubscriptionOptions
    {
        private readonly HashSet<string> _topics = new HashSet<string>();
        public TimeSpan MaxTimeoutToStop { get; set; } = TimeSpan.FromSeconds(1);
        public OnMessageReceived MessageReceivedHandler { get; private set; }
        public BatchOptions BatchOptions { get; } = new BatchOptions();

        public IncomingMessageConverter IncomingMessageConverter { get; set; } = new IncomingMessageConverter();

        public IEnumerable<string> Topics => _topics;

        public SubscriptionOptions To(params  string[] topics)
        {
            foreach (var topic in topics)
            {
                _topics.Add(topic);
            }
            return this;
        }

        public SubscriptionOptions OnMessageReceived(OnMessageReceived handler)
        {
            MessageReceivedHandler = handler;
            return this;
        }

        public SubscriptionOptions WithBatchOptions(Action<BatchOptions> configure)
        {
            configure(BatchOptions);
            return this;
        }
    }
}