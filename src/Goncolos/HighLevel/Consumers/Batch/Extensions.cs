using System;
using Goncolos.Consumers.Configuration;
using Goncolos.HighLevel.Consumers.Single;
using Goncolos.Infra.Pipeline;

namespace Goncolos.HighLevel.Consumers.Batch
{
    public static class Extensions
    {
        public static SubscriptionOptions UseBatchConsumerPipeline(this SubscriptionOptions options, Pipeline<BatchIncomingMessageContext> pipeline)
        {
            options.OnMessageReceived((messages, token) => pipeline.Execute(new BatchIncomingMessageContext(messages, token)));
            return options;
        }

        public static SubscriptionOptions UseBatchConsumerPipeline(this SubscriptionOptions options, Action<PipelineBuilder<BatchIncomingMessageContext>> pipelineConfiguration)
        {
            var pipelineBuilder = new PipelineBuilder<BatchIncomingMessageContext>();
            pipelineConfiguration?.Invoke(pipelineBuilder);
            var pipeline = pipelineBuilder.Build();
            return options.UseBatchConsumerPipeline(pipeline);
        }

        public static PipelineBuilder<BatchIncomingMessageContext> UseBatchToSingleMessageExecutor(this PipelineBuilder<BatchIncomingMessageContext> builder,
            Pipeline<SingleIncomingMessageContext> pipeline,
            Action<IncomingMessageToPipelineExecutor> executorConfiguration = null)
        {
            var executor = new IncomingMessageToPipelineExecutor(pipeline);
            executorConfiguration?.Invoke(executor);
            return builder
                .Use(async (context, next) =>
                {
                    await executor.Execute(context.IncomingMessages, context.CancellationToken, context.Items);
                    await next();
                });
        }

        public static PipelineBuilder<BatchIncomingMessageContext> UseBatchToSingleMessageExecutor(this PipelineBuilder<BatchIncomingMessageContext> builder,
            Action<PipelineBuilder<SingleIncomingMessageContext>> pipelineConfiguration,
            Action<IncomingMessageToPipelineExecutor> executorConfiguration = null)
        {
            var pipelineBuilder = new PipelineBuilder<SingleIncomingMessageContext>();
            pipelineConfiguration?.Invoke(pipelineBuilder);
            var pipeline = pipelineBuilder.Build();
            return UseBatchToSingleMessageExecutor(builder, pipeline, executorConfiguration);
        }
    }
}