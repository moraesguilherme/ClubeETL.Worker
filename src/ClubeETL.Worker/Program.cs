using ClubeETL.Worker.Configuration;
using ClubeETL.Worker.Parsing;
using ClubeETL.Worker.Persistence;
using ClubeETL.Worker.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.Configure<EtlOptions>(builder.Configuration.GetSection(EtlOptions.SectionName));
builder.Services.AddSingleton<IValidateOptions<EtlOptions>, EtlOptionsValidator>();

builder.Services.AddSingleton<HotelWorkbookParser>();
builder.Services.AddSingleton<CrecheWorkbookParser>();
builder.Services.AddSingleton<ISpreadsheetImportService, SpreadsheetImportService>();
builder.Services.AddSingleton<IEtlRepository, EtlRepository>();

builder.Services.AddHostedService<WorkerService>();

var host = builder.Build();
await host.RunAsync();