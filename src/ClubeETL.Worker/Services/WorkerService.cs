using ClubeETL.Worker.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ClubeETL.Worker.Services;

public sealed class WorkerService : BackgroundService
{
    private readonly ISpreadsheetImportService _spreadsheetImportService;
    private readonly IHostApplicationLifetime _lifetime;
    private readonly EtlOptions _options;
    private readonly ILogger<WorkerService> _logger;

    public WorkerService(
        ISpreadsheetImportService spreadsheetImportService,
        IHostApplicationLifetime lifetime,
        IOptions<EtlOptions> options,
        ILogger<WorkerService> logger)
    {
        _spreadsheetImportService = spreadsheetImportService;
        _lifetime = lifetime;
        _options = options.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var watchMode = _options.Mode.Equals("Watch", StringComparison.OrdinalIgnoreCase);

        if (!watchMode)
        {
            _logger.LogInformation("Executando em modo Manual.");
            await _spreadsheetImportService.ImportPendingFilesAsync(stoppingToken);
            _lifetime.StopApplication();
            return;
        }

        _logger.LogInformation("Executando em modo Watch. Intervalo: {Seconds}s", _options.PollingIntervalSeconds);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await _spreadsheetImportService.ImportPendingFilesAsync(stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Erro nao tratado no loop do worker.");
            }

            await Task.Delay(TimeSpan.FromSeconds(_options.PollingIntervalSeconds), stoppingToken);
        }
    }
}