using Xunit;

namespace Goncolos.AcceptanceTests
{
    [CollectionDefinition(nameof(KafkaSharedFixtureCollection))]
    public class KafkaSharedFixtureCollection : ICollectionFixture<KafkaSharedFixture>
    {
        
    }
}