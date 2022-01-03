using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Confluent.Kafka;
using Goncolos.Infra;
using Goncolos.Producers;
using Newtonsoft.Json;
using Headers = Goncolos.HighLevel.Headers;

namespace Goncolos.Benchmark
{
    [MemoryDiagnoser]
    public class ProducerBenchmark
    {
        private readonly OutgoingMessage[] _outgoingMessages;
        public string Servers = Environment.GetEnvironmentVariable("Goncolos.Benchmark.Brokers");
        public string Topic = Environment.GetEnvironmentVariable("Goncolos.Benchmark.Topic");

        public ProducerBenchmark()
        {
            var messages = Enumerable
                .Range(0, SampleSize)
                .Select(i => new SimpleMessage
                {
                    Id = i
                })
                .ToArray();
            _outgoingMessages = messages
                .Select(m => new OutgoingMessage(new TopicWithPartition(Topic), Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(m)), new Dictionary<string, string>()
                {
                    [Headers.PublishedAt] = SystemTime.UtcNowOffset.ToString("O")
                }))
                .ToArray();
        }

        public int SampleSize { get; set; } = 100_000;
        public int ChunkSize { get; set; } = 10_000;


        public async Task ProduceChunkByChunk()
        {
            var config = new KafkaProducerConfiguration(Servers)
            {
                BatchSize = 1_000_000,
                LingerMs = 20,
                CompressionType = CompressionType.Lz4,
                CompressionLevel = 10,
                Acks = Acks.All
            };

            using var producer = new KafkaProducer(config);


            var chunks = _outgoingMessages
                .Select((s, i) => new {Value = s, Index = i})
                .GroupBy(x => x.Index / ChunkSize)
                .Select(grp => grp.Select(x => x.Value).ToArray())
                .ToArray();
            Console.WriteLine("starting ProduceChunkByChunk..");

            var sw = new Stopwatch();
            sw.Start();
            foreach (var chunk in chunks)
            {
                await producer.ProduceBulk(chunk);
            }

            sw.Stop();
            Console.WriteLine($"produce completed at {sw.Elapsed}");
        }


        [Benchmark]
        public async Task ProduceCallback()
        {
            var config = new KafkaProducerConfiguration(Servers)
            {
                QueueBufferingMaxMessages = 1_000_000,
                BatchSize = 20_000_000,
                MessageSendMaxRetries = 2,
                RetryBackoffMs = 10,
                LingerMs = 10,
                DeliveryReportFields = "none",
                Acks = Acks.All,
                CompressionType = CompressionType.Lz4,
                CompressionLevel = 10
            };
            using var producer = new KafkaProducer(config);

            Console.WriteLine("starting ProduceCallback..");
            var sw = new Stopwatch();
            sw.Start();
            await producer.ProduceBulkWithCallback(_outgoingMessages);
            sw.Stop();
            Console.WriteLine($"produce completed at {sw.Elapsed}");
        }

        [Benchmark(Baseline = true)]
        public async Task ProduceAll()
        {
            var config = new KafkaProducerConfiguration(Servers)
            {
                QueueBufferingMaxMessages = 1_000_000,
                BatchSize = 20_000_000,
                MessageSendMaxRetries = 2,
                RetryBackoffMs = 10,
                LingerMs = 10,
                DeliveryReportFields = "none",
                Acks = Acks.All,
                CompressionType = CompressionType.Lz4,
                CompressionLevel = 10
            };
            using var producer = new KafkaProducer(config);


            Console.WriteLine("starting ProduceAll..");
            var sw = new Stopwatch();
            sw.Start();
            await producer.ProduceBulk(_outgoingMessages);
            sw.Stop();
            Console.WriteLine($"produce completed at {sw.Elapsed}");
        }
    }
}