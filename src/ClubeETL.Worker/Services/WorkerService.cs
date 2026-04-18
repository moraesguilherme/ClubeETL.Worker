using ClubeETL.Worker.Configuration;
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
        var watchMode = string.Equals(_options.Mode, "Watch", StringComparison.OrdinalIgnoreCase);

        if (!watchMode)
        {
            _logger.LogInformation("Executando em modo Manual.");

            var pendingFiles = GetPendingFiles();

            if (pendingFiles.Count == 0)
            {
                _logger.LogInformation("Nenhum arquivo pendente encontrado em {InputFolder}", _options.InputFolder);
                _lifetime.StopApplication();
                return;
            }

            foreach (var filePath in pendingFiles)
            {
                if (stoppingToken.IsCancellationRequested)
                {
                    break;
                }

                try
                {
                    await _spreadsheetImportService.ProcessFileAsync(filePath, stoppingToken);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Erro ao processar arquivo {FilePath} no modo Manual.", filePath);
                }
            }

            _lifetime.StopApplication();
            return;
        }

        _logger.LogInformation(
            "Executando em modo Watch. Intervalo: {Seconds}s. Pasta monitorada: {InputFolder}",
            _options.PollingIntervalSeconds,
            _options.InputFolder);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var pendingFiles = GetPendingFiles();

                if (pendingFiles.Count == 0)
                {
                    _logger.LogDebug("Nenhum arquivo pendente encontrado.");
                }
                else
                {
                    foreach (var filePath in pendingFiles)
                    {
                        if (stoppingToken.IsCancellationRequested)
                        {
                            break;
                        }

                        try
                        {
                            await _spreadsheetImportService.ProcessFileAsync(filePath, stoppingToken);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "Erro ao processar arquivo {FilePath} no modo Watch.", filePath);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Erro nao tratado no loop do worker.");
            }

            await Task.Delay(TimeSpan.FromSeconds(_options.PollingIntervalSeconds), stoppingToken);
        }
    }

    private List<string> GetPendingFiles()
    {
        if (string.IsNullOrWhiteSpace(_options.InputFolder))
        {
            _logger.LogWarning("InputFolder nao configurada.");
            return [];
        }

        if (!Directory.Exists(_options.InputFolder))
        {
            _logger.LogWarning("Pasta de entrada nao encontrada: {InputFolder}", _options.InputFolder);
            return [];
        }

        var allowedExtensions = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            ".xlsx",
            ".xls"
        };

        if (_options.IncludeCsv)
        {
            allowedExtensions.Add(".csv");
        }

        return Directory
            .EnumerateFiles(_options.InputFolder, "*.*", SearchOption.TopDirectoryOnly)
            .Where(path => allowedExtensions.Contains(Path.GetExtension(path)))
            .OrderBy(path => path, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }
}