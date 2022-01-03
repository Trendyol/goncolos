using System;
using System.Threading.Tasks;
using Goncolos.HighLevel;
using Goncolos.HighLevel.Consumers.Single;
using Goncolos.Infra;
using Shouldly;
using Xunit;

namespace Goncolos.Tests.HighLevel.IncomingPipelineSteps
{
    public class DelayStepTests : PipelineStepTestBase
    {
        private DelayStep _step;

        public DelayStepTests()
        {
            _step = new DelayStep();
        }

        [Fact]
        public async Task should_delay_before_calling_next_step()
        {
            var incomingMessage = CreateIncomingMessage(new SimpleMessage() {Text = "hello world", Number = 1});
            incomingMessage.Headers[Headers.PublishedAt] = "2020-02-06T23:00:00.0000000+00:00";
            incomingMessage.Headers[Headers.DelaySeconds] = "3";
            var callingTime = SystemTime.UtcNowOffset;
            
            var fakeTime = FakeDateTime(DateTimeOffset.Parse("2020-02-06T23:00:01.0000000+00:00"));
            await _step.Execute(new SingleIncomingMessageContext(incomingMessage), context =>
            {
                fakeTime.Dispose();
                var diff = SystemTime.UtcNowOffset - callingTime;
                diff.TotalSeconds.ShouldBeGreaterThanOrEqualTo(2);
                return Task.CompletedTask;
            });
        }
    }
}