using System;

namespace Goncolos.Consumers.Configuration
{
    public class BatchOptions
    {
        public TimeSpan BatchTimeout { get; set; } = TimeSpan.FromSeconds(10);
        public int BatchSize { get; set; } = 1;
        public int InputQueueMaxSize { get; set; } = 100;
    }
}