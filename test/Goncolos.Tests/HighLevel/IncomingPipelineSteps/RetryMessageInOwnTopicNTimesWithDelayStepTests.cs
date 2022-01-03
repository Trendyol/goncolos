using System;
using FakeItEasy;
using Goncolos.HighLevel.Consumers.Single;
using Goncolos.Producers;
using Microsoft.Extensions.Logging.Abstractions;
using Shouldly;
using Xunit;

namespace Goncolos.Tests.HighLevel.IncomingPipelineSteps
{
    public class RetryMessageInOwnTopicNTimesWithDelayStepTests : PipelineStepTestBase
    {
        [Fact]
        public  void should_return_zero_delay_when_diff_between_published_and_delay_is_equal()
        {
            var now = DateTimeOffset.Parse("2020-01-26T16:15:00.0000000+00:00");
            FakeDateTime(now);
            
            var step = new RetryMessageInOwnTopicNTimesWithDelayStep(NullLogger.Instance, A.Fake<IKafkaProducer>(), new RetryMessageInOwnTopicNTimesWithDelayStepOptions(TimeSpan.FromMinutes(5)));
            var publishedAt = DateTimeOffset.Parse("2020-01-26T16:10:00.0000000+00:00");
            step.GetDelay(publishedAt).ShouldBe(TimeSpan.Zero);
        }
        
        [Fact]
        public  void should_return_diff_when_there_is_a_diff()
        {
            var now = DateTimeOffset.Parse("2020-01-26T16:14:00.0000000+00:00");
            FakeDateTime(now);
            
            var step = new RetryMessageInOwnTopicNTimesWithDelayStep(NullLogger.Instance, A.Fake<IKafkaProducer>(), new RetryMessageInOwnTopicNTimesWithDelayStepOptions( TimeSpan.FromMinutes(5)));
            var publishedAt = DateTimeOffset.Parse("2020-01-26T16:10:00.0000000+00:00");
            step.GetDelay(publishedAt).ShouldBe(TimeSpan.FromMinutes(1));
        }
        
        [Fact]
        public  void should_return_delay_when_consumers_clock_is_behind_from_publisher_clock ()
        {
            var now = DateTimeOffset.Parse("2020-01-26T16:00:00.0000000+00:00");
            FakeDateTime(now);
            
            var step = new RetryMessageInOwnTopicNTimesWithDelayStep(NullLogger.Instance, A.Fake<IKafkaProducer>(), new RetryMessageInOwnTopicNTimesWithDelayStepOptions(TimeSpan.FromMinutes(5)));
            var publishedAt = DateTimeOffset.Parse("2020-01-26T16:15:00.0000000+00:00");
            step.GetDelay(publishedAt).ShouldBe(TimeSpan.FromMinutes(5));
        }
    }
}