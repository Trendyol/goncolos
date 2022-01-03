using System;
using System.Diagnostics;
using System.Reflection;
using System.Text;

namespace Goncolos
{
    public static class Constants
    {
        public static string Host { get; }
        public static string AssemblyVersion { get; }
        public static  string  ClientId { get; }
        
        public static readonly Encoding HeaderEncoding = Encoding.UTF8;
    

        static Constants()
        {
            AssemblyVersion = GetVersion();
            Host= Environment.MachineName;
            ClientId = $"{Host}-Goncolos@{AssemblyVersion}-{Guid.NewGuid().ToString("N")[..5]}";
        }

        private static string GetVersion()
        {
            var assembly = Assembly.GetExecutingAssembly();
            var fvi = FileVersionInfo.GetVersionInfo(assembly.Location);
            var version = fvi.FileVersion;
            return version;
        }

        public static class ConsumerLoggerFields
        {
            public static string ClientId = "Kafka-ClientId";
            public static string ConsumerName = "Kafka-ConsumerName";
            public static string Group = "Kafka-Group";
        }

       
    }
}