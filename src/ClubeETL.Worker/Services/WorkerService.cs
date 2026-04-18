using ClubeETL.Worker.Configuration;
using ClubeETL.Worker.Models;
using ClubeETL.Worker.Utils;
using Microsoft.Extensions.Options;

namespace ClubeETL.Worker.Services;

public sealed class WorkerService : BackgroundService
{
    private readonly ISpreadsheetImportService _spreadsheetImportService;
    private readonly IProcessedFileStateStore _processedFileStateStore;
    private readonly IHostApplicationLifetime _lifetime;
    private readonly EtlOptions _options;
    private readonly ILogger<WorkerService> _logger;

    public WorkerService(
        ISpreadsheetImportService spreadsheetImportService,
        IProcessedFileStateStore processedFileStateStore,
        IHostApplicationLifetime lifetime,
        IOptions<EtlOptions> options,
        ILogger<WorkerService> logger)
    {
        _spreadsheetImportService = spreadsheetImportService;
        _processedFileStateStore = processedFileStateStore;
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

            foreach (var filePath in pendingFiles)
            {
                if (stoppingToken.IsCancellationRequested)
                {
                    break;
                }

                await TryProcessFileAsync(filePath, stoppingToken);
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

                foreach (var filePath in pendingFiles)
                {
                    if (stoppingToken.IsCancellationRequested)
                    {
                        break;
                    }

                    await TryProcessFileAsync(filePath, stoppingToken);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Erro nao tratado no loop do worker.");
            }

            await Task.Delay(TimeSpan.FromSeconds(_options.PollingIntervalSeconds), stoppingToken);
        }
    }

    private async Task TryProcessFileAsync(string filePath, CancellationToken stoppingToken)
    {
        if (FileSnapshotHelper.IsTemporaryOfficeFile(filePath))
        {
            _logger.LogDebug("Arquivo temporario do Office ignorado: {FilePath}", filePath);
            return;
        }

        if (!File.Exists(filePath))
        {
            return;
        }

        var fingerprint = FileProcessingFingerprint.FromFile(filePath);

        if (_processedFileStateStore.HasSameSuccessfulFingerprint(fingerprint))
        {
            _logger.LogDebug(
                "Arquivo sem alteracao fisica desde a ultima execucao bem-sucedida. Ignorado: {FilePath}",
                filePath);
            return;
        }

        string? snapshotPath = null;

        try
        {
            snapshotPath = FileSnapshotHelper.CreateSnapshotCopy(filePath);

            await _spreadsheetImportService.ProcessFileAsync(snapshotPath, filePath, stoppingToken);

            _processedFileStateStore.MarkAsSuccessfullyProcessed(fingerprint);
            await _processedFileStateStore.SaveAsync(stoppingToken);
        }
        catch (IOException ex)
        {
            _logger.LogWarning(
                ex,
                "Arquivo indisponivel para snapshot/leitura neste ciclo: {FilePath}",
                filePath);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Erro ao processar arquivo {FilePath}.", filePath);
        }
        finally
        {
            FileSnapshotHelper.TryDeleteSnapshot(snapshotPath);
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

        return Directory
            .EnumerateFiles(_options.InputFolder, "*.*", SearchOption.TopDirectoryOnly)
            .Where(path => allowedExtensions.Contains(Path.GetExtension(path)))
            .OrderBy(path => path, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }
}