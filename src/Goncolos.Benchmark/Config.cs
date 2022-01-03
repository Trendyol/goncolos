using System.Collections.Immutable;
using System.Diagnostics.Tracing;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Exporters.Csv;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Loggers;
using BenchmarkDotNet.Order;
using Microsoft.Diagnostics.NETCore.Client;
using Microsoft.Diagnostics.Tracing.Parsers;

namespace Goncolos.Benchmark
{
    public class Config : ManualConfig
    {
        public const int Iterations = 1;

        public Config()
        {
            AddLogger(ConsoleLogger.Default);

            AddExporter(CsvExporter.Default);
            AddExporter(HtmlExporter.Default);
            AddExporter(MarkdownExporter.Default);


            AddDiagnoser(MemoryDiagnoser.Default);
            AddDiagnoser(new EventPipeProfiler(EventPipeProfile.CpuSampling));
            AddDiagnoser(ThreadingDiagnoser.Default);
            AddColumn(TargetMethodColumn.Method);
            AddColumn(StatisticColumn.Mean);
            AddColumn(StatisticColumn.StdDev);
            AddColumn(StatisticColumn.Error);
            AddColumn(BaselineRatioColumn.RatioMean);
            AddColumnProvider(DefaultColumnProviders.Metrics);

            AddJob(Job.ShortRun
                .WithLaunchCount(1)
                .WithWarmupCount(1)
                .WithUnrollFactor(Iterations)
                .WithIterationCount(1)
            );
            Orderer = new DefaultOrderer(SummaryOrderPolicy.FastestToSlowest);
            Options |= ConfigOptions.JoinSummary;
        }
    }
}