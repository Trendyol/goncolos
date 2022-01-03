using System;
using System.Diagnostics;
using System.Net.Http;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using TestEnvironment.Docker;
using TestEnvironment.Docker.Containers.Kafka;
using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace Goncolos.AcceptanceTests
{
    public class KafkaSharedFixture : IAsyncLifetime
    {
        private  ITestOutputHelper _testOutputHelper = new TestOutputHelper();
        private DockerEnvironment _environment;
        private bool _isDebug;

        public KafkaSharedFixture()
        {
            SetIsDebugging();
            Name = "test";
        }

        public async Task WithAdminClient(Func<IAdminClient, Task> action)
        {
            using var adminClient = new AdminClientBuilder(new AdminClientConfig
                {
                    BootstrapServers = KafkaBootstrapServers,
                })
                .Build();
            await action(adminClient);
        }
        public KafkaSharedFixture Inject(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper ?? throw new ArgumentNullException(nameof(testOutputHelper));
            return this;
        }
        
        public string Name { get; }
        public ILogger Logger => LoggerFactory.Create(lb => lb.AddConsole().AddDebug()).CreateLogger<KafkaSharedFixture>();
        public string KafkaBootstrapServers { get; private set; }

        public async Task InitializeAsync()
        {
            var builder = CreateTestEnvironmentBuilder();

            Logger.LogInformation("EnvironmentContext initializing...");

            _environment = builder.Build();
            await _environment.Up();

            var kafkaContainer = _environment.GetContainer<KafkaContainer>("local-kafka");
            KafkaBootstrapServers = kafkaContainer.GetUrl();
            Logger.LogInformation("EnvironmentContext initialized.");
            await OnInitialized();
        }

        [Conditional("DEBUG")]
        public void SetIsDebugging()
        {
            _isDebug = true;
        }

        public async Task DisposeAsync()
        {
            if (!_isDebug)
            {
                await _environment.DisposeAsync();
            }
        }

        protected virtual Task OnInitialized()
        {
            return Task.CompletedTask;
        }

        private IDockerEnvironmentBuilder CreateTestEnvironmentBuilder()
        {
            var reuseContainer = _isDebug;
            return new DockerEnvironmentBuilder()
                .SetName(Name)
                .AddKafkaContainer("local-kafka", reuseContainer: reuseContainer);
        }
    }
}