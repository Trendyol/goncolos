using BenchmarkDotNet.Running;

namespace Goncolos.Benchmark
{
    public class Program
    {
        private static void Main(string[] args)
        {
            new BenchmarkSwitcher(typeof(Program).Assembly).Run(args, new Config());
        }
    }
}