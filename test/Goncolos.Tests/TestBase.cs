using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using FakeItEasy;
using Goncolos.Infra;

namespace Goncolos.Tests
{
    public class TestBase
    {
        public T Ignored<T>() => A<T>._;

        public CancellationToken IgnoredCancellationToken()
        {
            return A<CancellationToken>._;
        }

        public T Matches<T>(Expression<Func<T, bool>> predicate) => A<T>.That.Matches(predicate);

        public IEnumerable<T> IgnoredEnumerable<T>() => A<IEnumerable<T>>._;
        public INegatableArgumentConstraintManager<IEnumerable<T>> EnumerableOf<T>() => A<IEnumerable<T>>.That;
        
        public IDisposable FakeDateTime(DateTimeOffset dt) => SystemTimeFaker.FakeIt(dt);
        public IDisposable FakeDateTimeWithNow() => SystemTimeFaker.FakeIt(SystemTime.UtcNow);

        private class SystemTimeFaker
        {
            public static IDisposable FakeIt(DateTimeOffset fakeTime)
            {
                return new FakeTime(fakeTime);
            }

            private class FakeTime
                : IDisposable
            {
                public FakeTime(DateTimeOffset fakeTime)
                {
                    SystemTime.Set(() =>
                    {
                        var timeToReturn = fakeTime;
                        return timeToReturn;
                    });
                }

                public void Dispose()
                {
                    SystemTime.Reset();
                }
            }
        }
    }
}