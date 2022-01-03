using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using FakeItEasy;
using Goncolos.Consumers;
using Goncolos.Consumers.Configuration;
using Nito.AsyncEx;
using Shouldly;
using Xunit;

namespace Goncolos.Tests.Consumers
{
    public class ChannelBasedWorkerTests : TestBase
    {
        public ChannelBasedWorkerTests()
        {
            _kafkaConsumer = A.Fake<IKafkaConsumer>();
            _consumer = A.Fake<IConsumer<string, byte[]>>();
            _topicStateManager = A.Fake<ITopicStateManager>();
        }

        private readonly IKafkaConsumer _kafkaConsumer;
        private readonly IConsumer<string, byte[]> _consumer;
        private static readonly DateTimeOffset _messageTimestamp = DateTimeOffset.Parse("2021-02-21T17:23:48.000Z");
        private readonly ITopicStateManager _topicStateManager;

        private static IncomingMessage CreateIncomingMessage(long offset)
        {
            return new IncomingMessageBuilder
            {
                Body = Encoding.UTF8.GetBytes("hello world"),
                Headers = new Dictionary<string, string>
                {
                    ["test-header"] = "header-value"
                },
                Key = "test-key",
                Offset = offset,
                Timestamp = _messageTimestamp,
                Topic = new TopicWithPartition("test-topic", 1)
            }.Build();
        }

        private static ConsumeResult<string, byte[]> CreateConsumeResult(TopicPartition topicPartition, long offset)
        {
            return new ConsumeResult<string, byte[]>
            {
                Message = new Message<string, byte[]>
                {
                    Headers = new Headers
                    {
                        new Header("test-header", Encoding.UTF8.GetBytes("header-value"))
                    },
                    Key = "test-key",
                    Value = Encoding.UTF8.GetBytes("hello world"),
                    Timestamp = new Timestamp(_messageTimestamp)
                },
                Offset = new Offset(offset),
                Partition = topicPartition.Partition,
                Topic = topicPartition.Topic,
                IsPartitionEOF = false
            };
        }

        [Fact]
        public async Task should_consume_messages_as_batch()
        {
            var ce = new AsyncCountdownEvent(2);
            var chunks = new List<IncomingMessage[]>();

            Task OnMessageReceived(IncomingMessage[] messages, CancellationToken ct)
            {
                chunks.Add(messages);
                ce.Signal();
                return Task.CompletedTask;
            }

            var topicPartition = new TopicPartition("test-topic", 1);
            var configuration = new KafkaConsumerConfiguration("servers", "group")
                .Subscribe(s => s.To("test-topic").OnMessageReceived(OnMessageReceived).WithBatchOptions(o => { o.BatchSize = 5; }));
            var sut = new ChannelBasedWorker(_topicStateManager, topicPartition, configuration);

            await sut.Enqueue(CreateConsumeResult(topicPartition, 1));
            await sut.Enqueue(CreateConsumeResult(topicPartition, 2));
            await sut.Enqueue(CreateConsumeResult(topicPartition, 3));
            await sut.Enqueue(CreateConsumeResult(topicPartition, 4));
            await sut.Enqueue(CreateConsumeResult(topicPartition, 5));
            await sut.Enqueue(CreateConsumeResult(topicPartition, 6));
            await sut.Enqueue(CreateConsumeResult(topicPartition, 7));
            await sut.Enqueue(CreateConsumeResult(topicPartition, 8));
            await sut.Enqueue(CreateConsumeResult(topicPartition, 9));
            await sut.Enqueue(CreateConsumeResult(topicPartition, 10));

            await ce.WaitAsync();
            chunks[0].Length.ShouldBe(5);
            chunks[0][0].ShouldBeEqual(CreateIncomingMessage(1));
            chunks[0][1].ShouldBeEqual(CreateIncomingMessage(2));
            chunks[0][2].ShouldBeEqual(CreateIncomingMessage(3));
            chunks[0][3].ShouldBeEqual(CreateIncomingMessage(4));
            chunks[0][4].ShouldBeEqual(CreateIncomingMessage(5));
            chunks[1].Length.ShouldBe(5);
            chunks[1][0].ShouldBeEqual(CreateIncomingMessage(6));
            chunks[1][1].ShouldBeEqual(CreateIncomingMessage(7));
            chunks[1][2].ShouldBeEqual(CreateIncomingMessage(8));
            chunks[1][3].ShouldBeEqual(CreateIncomingMessage(9));
            chunks[1][4].ShouldBeEqual(CreateIncomingMessage(10));
        }

        [Fact]
        public async Task should_consume_queued_messages_when_batch_size_not_ready()
        {
            var waitForCompletion = new TaskCompletionSource<IncomingMessage[]>(TaskCreationOptions.RunContinuationsAsynchronously);

            Task OnMessageReceived(IncomingMessage[] messages, CancellationToken ct)
            {
                waitForCompletion.SetResult(messages);
                return Task.CompletedTask;
            }

            var topicPartition = new TopicPartition("test-topic", 1);
            var configuration = new KafkaConsumerConfiguration("servers", "group")
                .Subscribe(s => s.To("test-topic")
                    .OnMessageReceived(OnMessageReceived)
                    .WithBatchOptions(o =>
                    {
                        o.BatchSize = 5;
                        o.BatchTimeout = TimeSpan.FromSeconds(1);
                    }));
            var sut = new ChannelBasedWorker(_topicStateManager, topicPartition, configuration);

            await sut.Enqueue(CreateConsumeResult(topicPartition, 1));
            await sut.Enqueue(CreateConsumeResult(topicPartition, 2));
            await sut.Enqueue(CreateConsumeResult(topicPartition, 3));

            var incomingMessages = await waitForCompletion.Task;
            incomingMessages.Length.ShouldBe(3);
            incomingMessages[0].ShouldBeEqual(CreateIncomingMessage(1));
            incomingMessages[1].ShouldBeEqual(CreateIncomingMessage(2));
            incomingMessages[2].ShouldBeEqual(CreateIncomingMessage(3));
        }

        [Fact]
        public void should_create()
        {
            var topicPartition = new TopicPartition("test-topic", 1);
            var configuration = new KafkaConsumerConfiguration("servers", "group")
                .Subscribe(s => s.To("test-topic").OnMessageReceived((messages, token) => Task.CompletedTask));
            var sut = new ChannelBasedWorker(_topicStateManager, topicPartition, configuration);
            sut.ShouldNotBeNull();
        }


        [Fact]
        public async Task should_dispose_queue_when_occurred_unhandled_exception_on_processing_message()
        {
            Task OnMessageReceived(IncomingMessage[] messages, CancellationToken ct)
            {
                throw new Exception("unhandled exception");
            }

            var topicPartition = new TopicPartition("test-topic", 1);
            var configuration = new KafkaConsumerConfiguration("servers", "group")
                .Subscribe(s => s.To("test-topic").OnMessageReceived(OnMessageReceived).WithBatchOptions(o =>
                {
                    o.BatchSize = 3;
                    o.InputQueueMaxSize = 3;
                }));
            var sut = new ChannelBasedWorker(_topicStateManager, topicPartition, configuration);

            await sut.Enqueue(CreateConsumeResult(topicPartition, 1));
            await sut.Enqueue(CreateConsumeResult(topicPartition, 2));
            await sut.Enqueue(CreateConsumeResult(topicPartition, 3));

            await Task.Delay(200);
            try
            {
                await sut.Enqueue(CreateConsumeResult(topicPartition, 4));
            }
            catch (Exception e)
            {
                e.ShouldBeAssignableTo<ObjectDisposedException>();
            }
        }

        [Fact]
        public async ValueTask should_enqueue_message()
        {
            var topicPartition = new TopicPartition("test-topic", 1);
            var configuration = new KafkaConsumerConfiguration("servers", "group")
                .Subscribe(s => s.To("test-topic").OnMessageReceived((messages, token) => Task.CompletedTask));
            var sut = new ChannelBasedWorker(_topicStateManager, topicPartition, configuration);
            await sut.Enqueue(CreateConsumeResult(topicPartition, 1));
        }

        [Fact]
        public void should_not_equal_two_same_topic_partition_queue()
        {
            var topicPartition = new TopicPartition("test-topic", 1);
            var configuration = new KafkaConsumerConfiguration("servers", "group")
                .Subscribe(s => s.To("test-topic").OnMessageReceived((messages, token) => Task.CompletedTask));
            var sut = new ChannelBasedWorker(_topicStateManager, topicPartition, configuration);

            var sut2 = new ChannelBasedWorker(_topicStateManager, topicPartition, configuration);
            sut.ShouldNotBe(sut2);
        }


        [Fact]
        public async ValueTask should_pause_consumer_when_queue_is_full()
        {
            var topicPartition = new TopicPartition("test-topic", 1);
            var configuration = new KafkaConsumerConfiguration("servers", "group")
                .Subscribe(s => s.To("test-topic").OnMessageReceived((messages, token) => Task.CompletedTask).WithBatchOptions(o => { o.InputQueueMaxSize = 5; }));
            var sut = new ChannelBasedWorker(_topicStateManager, topicPartition, configuration);

            await sut.Enqueue(CreateConsumeResult(topicPartition, 1));
            await sut.Enqueue(CreateConsumeResult(topicPartition, 2));
            await sut.Enqueue(CreateConsumeResult(topicPartition, 3));
            await sut.Enqueue(CreateConsumeResult(topicPartition, 4));
            await sut.Enqueue(CreateConsumeResult(topicPartition, 5));

            A.CallTo(() => _consumer.Pause(new[] { topicPartition })).MustHaveHappenedOnceExactly();
        }


        [Fact]
        public async Task should_resume_consumer_after_processing_messages()
        {
            var tsc = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

            Task OnMessageReceived(IncomingMessage[] messages, CancellationToken ct)
            {
                return Task.CompletedTask;
            }

            var topicPartition = new TopicPartition("test-topic", 1);
            var configuration = new KafkaConsumerConfiguration("servers", "group")
                .Subscribe(s => s.To("test-topic").OnMessageReceived(OnMessageReceived).WithBatchOptions(o =>
                {
                    o.BatchSize = 1;
                    o.InputQueueMaxSize = 1;
                }));
            var sut = new ChannelBasedWorker(_topicStateManager, topicPartition, configuration);


            var calls = A.CallTo(() => _topicStateManager.Resume(A<TopicPartition>._));
            calls.Invokes((TopicPartition tp) =>
            {
                tp.ShouldBe(topicPartition);
                if (!tsc.Task.IsCompleted)
                {
                    tsc.TrySetResult(true);
                }
            });


            await sut.Enqueue(CreateConsumeResult(topicPartition, 1));
            await sut.Enqueue(CreateConsumeResult(topicPartition, 2));
            await sut.Enqueue(CreateConsumeResult(topicPartition, 3));
            await sut.Enqueue(CreateConsumeResult(topicPartition, 4));
            await sut.Enqueue(CreateConsumeResult(topicPartition, 5));
            await sut.Enqueue(CreateConsumeResult(topicPartition, 6));
            await sut.Enqueue(CreateConsumeResult(topicPartition, 7));

            await tsc.Task;
            calls.MustHaveHappenedOnceOrMore();
        }

        [Fact]
        public async Task should_store_max_offset_of_processed_messages()
        {
            var waitForCompletion = new TaskCompletionSource<IncomingMessage[]>(TaskCreationOptions.RunContinuationsAsynchronously);

            Task OnMessageReceived(IncomingMessage[] messages, CancellationToken ct)
            {
                waitForCompletion.SetResult(messages);
                return Task.CompletedTask;
            }

            var topicPartition = new TopicPartition("test-topic", 1);
            var configuration = new KafkaConsumerConfiguration("servers", "group")
                .Subscribe(s => s.To("test-topic").OnMessageReceived(OnMessageReceived).WithBatchOptions(o => { o.BatchSize = 3; }));
            var sut = new ChannelBasedWorker(_topicStateManager, topicPartition, configuration);

            await sut.Enqueue(CreateConsumeResult(topicPartition, 1));
            await sut.Enqueue(CreateConsumeResult(topicPartition, 2));
            await sut.Enqueue(CreateConsumeResult(topicPartition, 3));

            var incomingMessages = await waitForCompletion.Task;
            incomingMessages.Length.ShouldBe(3);

            var calls = A.CallTo(() => _topicStateManager.StoreOffset(new TopicPartitionOffset(topicPartition, new Offset(3))));

            calls.MustHaveHappenedOnceExactly();
        }

        [Fact]
        public async Task should_not_enqueue_message_when_worker_is_disposed()
        {
            Task OnMessageReceived(IncomingMessage[] messages, CancellationToken ct)
            {
                return Task.CompletedTask;
            }

            var topicPartition = new TopicPartition("test-topic", 1);
            var configuration = new KafkaConsumerConfiguration("servers", "group")
                .Subscribe(s => s.To("test-topic").OnMessageReceived(OnMessageReceived).WithBatchOptions(o => { o.BatchSize = 3; }));
            var sut = new ChannelBasedWorker(_topicStateManager, topicPartition, configuration);
            await sut.DisposeAsync();
            await sut.Enqueue(CreateConsumeResult(topicPartition, 1)).AsTask().ShouldThrowAsync<ObjectDisposedException>();
        }
    }
}