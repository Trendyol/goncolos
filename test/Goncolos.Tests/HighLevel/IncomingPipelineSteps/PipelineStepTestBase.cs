using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Goncolos.Consumers;
using Goncolos.HighLevel;
using Goncolos.HighLevel.Consumers.Single;
using Goncolos.Infra;
using Goncolos.Infra.Pipeline;
using Goncolos.Tests.Consumers;
using Newtonsoft.Json;

namespace Goncolos.Tests.HighLevel.IncomingPipelineSteps
{
    public class PipelineStepTestBase : TestBase
    {
        protected PipelineStepDelegate<SingleIncomingMessageContext> TerminateStep => ctx => Task.CompletedTask;

        protected IncomingMessage CreateIncomingMessage(object message, int offset = 1)
        {
            var body = GetBody(message);
            return new IncomingMessageBuilder
            {
                Body = body,
                Headers = new Dictionary<string, string>()
                {
                    [Headers.CorrelationId] = "123"
                },
                Key = message.GetType().Name,
                Offset = offset,
                Timestamp = SystemTime.UtcNowOffset,
                Topic = new TopicWithPartition("test", 1)
            }.Build();
        }

        private byte[] GetBody(object message)
        {
            return Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(message));
        }
    }
}