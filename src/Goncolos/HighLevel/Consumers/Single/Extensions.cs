using System;
using System.Threading;
using System.Threading.Tasks;
using Goncolos.Consumers;
using Goncolos.Consumers.Configuration;
using Goncolos.Infra.Pipeline;

namespace Goncolos.HighLevel.Consumers.Single
{
    public static class Extensions
    {
        public static SubscriptionOptions UseConsumerPipeline(this SubscriptionOptions options,
            Action<PipelineBuilder<SingleIncomingMessageContext>> pipelineConfiguration, Action<IncomingMessageToPipelineExecutor> executorConfiguration = null)
        {
            var pipelineBuilder = new PipelineBuilder<SingleIncomingMessageContext>();
            pipelineConfiguration?.Invoke(pipelineBuilder);
            var pipeline = pipelineBuilder.Build();
            var executor = new IncomingMessageToPipelineExecutor(pipeline);
            executorConfiguration?.Invoke(executor);
            options.OnMessageReceived((messages, token) => executor.Execute(messages,token));
            return options;
        }

        public static SubscriptionOptions ContinueWithPipeline(this SubscriptionOptions options,
            Action<PipelineBuilder<SingleIncomingMessageContext>> pipelineConfiguration, Action<IncomingMessageToPipelineExecutor> executorConfiguration = null)
        {
            var pipelineBuilder = new PipelineBuilder<SingleIncomingMessageContext>();
            pipelineConfiguration?.Invoke(pipelineBuilder);
            var pipeline = pipelineBuilder.Build();
            var executor = new IncomingMessageToPipelineExecutor(pipeline);
            executorConfiguration?.Invoke(executor);

            async Task OnMessageReceived(IncomingMessage[] incomingMessages, CancellationToken cancellationToken)
            {
                await options.MessageReceivedHandler(incomingMessages, cancellationToken);
                await executor.Execute(incomingMessages, cancellationToken);
            }

            options.OnMessageReceived(OnMessageReceived);
            return options;
        }
    }
}