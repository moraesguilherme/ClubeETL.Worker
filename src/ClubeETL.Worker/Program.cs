using ClubeETL.Worker.Configuration;
using ClubeETL.Worker.Models;
using ClubeETL.Worker.Persistence;
using ClubeETL.Worker.Parsing;
using ClubeETL.Worker.Services;
using Dapper;

internal class Program
{
    private static async Task Main(string[] args)
    {
        var builder = Host.CreateApplicationBuilder(args);

        DefaultTypeMap.MatchNamesWithUnderscores = true;

        builder.Services.Configure<EtlOptions>(
            builder.Configuration.GetSection(EtlOptions.SectionName));

        builder.Services.AddSingleton<UnifiedPaymentsWorkbookParser>();
        builder.Services.AddSingleton<IEtlRepository, EtlRepository>();
        builder.Services.AddSingleton<ISpreadsheetImportService, SpreadsheetImportService>();
        builder.Services.AddSingleton<IProcessedFileStateStore, ProcessedFileStateStore>();

        builder.Services.AddHostedService<WorkerService>();
        builder.Services.AddSingleton<SpreadsheetImportService>();

        var host = builder.Build();
        await host.RunAsync();
    }
}