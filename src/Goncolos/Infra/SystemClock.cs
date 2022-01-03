using System;
using System.Threading;

namespace Goncolos.Infra
{
    public class SystemTime
    {
        private static readonly Func<DateTimeOffset> DefaultTimeFactory = () => DateTimeOffset.Now;
        private static readonly AsyncLocal<Func<DateTimeOffset>> CurrentTimeFactoryLocal = new AsyncLocal<Func<DateTimeOffset>>();
        private static Func<DateTimeOffset> CurrentTimeFactory => CurrentTimeFactoryLocal.Value ?? DefaultTimeFactory;
        public static DateTimeOffset UtcNowOffset => CurrentTimeFactory().UtcDateTime;
        public static DateTime UtcNow => CurrentTimeFactory().UtcDateTime;

        public static void Reset()
        {
            CurrentTimeFactoryLocal.Value = DefaultTimeFactory;
        }

        public static void Set(Func<DateTimeOffset> factory)
        {
            CurrentTimeFactoryLocal.Value = factory;
        }
    }
}