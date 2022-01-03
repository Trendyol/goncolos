using System;
using Goncolos.Infra;
using Shouldly;
using Xunit;

namespace Goncolos.Tests.Infra
{
    public class SystemTimeTests
    {
        [Fact]
        public void should_fake_datetime()
        {
            var dt = DateTimeOffset.UtcNow;
            SystemTime.Set(()=>dt);
            SystemTime.UtcNowOffset.ShouldBe(dt);
            SystemTime.UtcNow.ShouldBe(dt.DateTime);
        }
    }
}