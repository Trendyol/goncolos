Goncolos
=====================================================

![example workflow name](https://github.com/Trendyol/goncolos/workflows/Test/badge.svg)

Goncolos is simple wrapper top of [confluent-kafka-dotnet](https://github.com/confluentinc/confluent-kafka-dotnet)

## Features:

- Bulk message consuming
- Bulk message producing
- Pipeline supports to process consumed messages 

## Usage

### Consumer Examples 

#### Using Mediator as Dispatcher

```csharp
public class KafkaConsumerService
{
    private readonly ILogger<KafkaConsumerService> _logger;
    private readonly IKafkaProducer _kafkaProducer;
    private readonly IMediator _mediator;
    private IKafkaConsumer _consumer;
    
    public KafkaConsumerService(ILogger<KafkaConsumerService> logger,IKafkaProducer kafkaProducer,IMediator mediator)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _kafkaProducer = kafkaProducer ?? throw new ArgumentNullException(nameof(kafkaProducer));
        _mediator = mediator ?? throw new ArgumentNullException(nameof(mediator));
    }

    public async Task Start(CancellationToken cancellationToken)
    {
        var config =  CreateConfig();
        _consumer = new KafkaConsumer(config);
        await _consumer.Start(cancellationToken);
    }
    
    public async Task Stop()
    {
        await _consumer.DisposeAsync();
    }

    private KafkaConsumerConfiguration CreateConfig()
    {
        var config = new KafkaConsumerConfiguration("localhost:9092", "consumer-group-name", "ProductCreated")
            .UseLogger(_logger)
            .ConfigureIncomingPipeline(pipe =>
                pipe
                    .UsePipelineStep(new SendFailedMessageToAnotherTopicStep(_logger, _kafkaProducer, new SendFailedMessageToAnotherTopicStepOptions("ProductCreatedErrorTopic")))
                    .UsePipelineStep(new MessageSerializerStep(new JsonMessageSerializer()))
                    .UsePipelineStep(new MediatrDispatchStep(_mediator))
            );
        return config;
    }

    public class MediatrDispatchStep
        :IPipelineStep<IncomingMessageContext>
    {
        private readonly IMediator _mediator;

        public MediatrDispatchStep(IMediator mediator)
        {
            _mediator = mediator ?? throw new ArgumentNullException(nameof(mediator));
        }
    
        public async Task Execute(IncomingMessageContext context, PipelineStepDelegate<IncomingMessageContext> next)
        {
            var message = context.Message;
            switch (message)
            {
                case IRequest<Unit> request:
                    await _mediator.Send(request, context.CancellationToken);
                    return;
                case IBaseRequest request:
                    await ((dynamic) _mediator).Send((dynamic) request,  context.CancellationToken);
                    return;
                default:
                    await _mediator.Publish(message,  context.CancellationToken);
                    break;
            }
            await next(context);
        }
    }
}
```