using System;
using System.Threading.Tasks;
using Goncolos.HighLevel.Serializations;
using Goncolos.Infra.Pipeline;

namespace Goncolos.HighLevel.Consumers.Single
{
    public class MessageSerializerStep
        : IPipelineStep<SingleIncomingMessageContext>
    {
        private readonly IMessageSerializer _messageSerializer;

        public MessageSerializerStep(IMessageSerializer messageSerializer)
        {
            _messageSerializer = messageSerializer ?? throw new ArgumentNullException(nameof(messageSerializer));
        }

        public async Task Execute(SingleIncomingMessageContext context, PipelineStepDelegate<SingleIncomingMessageContext> next)
        {
            var message = await _messageSerializer.Deserialize(context.IncomingMessage);
            if (message == null)
            {
                // ignore message
                return;
            }
            context.Message = message;
            await next(context);
        }
    }
}