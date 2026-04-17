using ClosedXML.Excel;
using ClubeETL.Worker.Configuration;
using ClubeETL.Worker.Models;
using ClubeETL.Worker.Parsing;
using ClubeETL.Worker.Persistence;
using ClubeETL.Worker.Utils;
using Microsoft.Extensions.Options;

namespace ClubeETL.Worker.Services;

public sealed class SpreadsheetImportService : ISpreadsheetImportService
{
    private readonly EtlOptions _options;
    private readonly IEtlRepository _repository;
    private readonly HotelWorkbookParser _hotelParser;
    private readonly CrecheWorkbookParser _crecheParser;
    private readonly ILogger<SpreadsheetImportService> _logger;

    public SpreadsheetImportService(
        IOptions<EtlOptions> options,
        IEtlRepository repository,
        HotelWorkbookParser hotelParser,
        CrecheWorkbookParser crecheParser,
        ILogger<SpreadsheetImportService> logger)
    {
        _options = options.Value;
        _repository = repository;
        _hotelParser = hotelParser;
        _crecheParser = crecheParser;
        _logger = logger;
    }

    public async Task ImportPendingFilesAsync(CancellationToken cancellationToken)
    {
        EnsureFolders();

        var files = Directory
            .EnumerateFiles(_options.InputFolder, "*.*", SearchOption.TopDirectoryOnly)
            .Where(IsSupportedFile)
            .OrderBy(x => x, StringComparer.OrdinalIgnoreCase)
            .ToList();

        if (files.Count == 0)
        {
            _logger.LogInformation("Nenhum arquivo pendente encontrado em {Folder}", _options.InputFolder);
            return;
        }

        foreach (var file in files)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await ProcessFileAsync(file, cancellationToken);
        }
    }

    private async Task ProcessFileAsync(string filePath, CancellationToken cancellationToken)
    {
        var fileName = Path.GetFileName(filePath);
        _logger.LogInformation("Processando arquivo {FileName}", fileName);

        Guid batchId = Guid.Empty;
        Guid runId = Guid.Empty;
        var importedRows = 0;
        var erroredRows = 0;
        var processedRows = 0;
        var finalStatus = "processed";
        string? finalNote = null;

        try
        {
            using var workbook = new XLWorkbook(filePath);

            var parserType = ResolveParserType(workbook);
            var reference = ResolveReference(workbook, filePath, parserType);

            batchId = await _repository.CreateBatchAsync(
                fileName,
                filePath,
                parserType,
                _options.SourceSystem,
                reference.ReferenceYear,
                reference.ReferenceMonth,
                cancellationToken);

            runId = await _repository.CreateProcessingRunAsync(batchId, fileName, cancellationToken);

            IReadOnlyList<ImportRowModel> rows = parserType switch
            {
                "hotel_agenda" => _hotelParser.Parse(filePath, workbook),
                "creche_mensal" => _crecheParser.Parse(filePath, workbook),
                _ => throw new InvalidOperationException($"Nenhum parser suportado para o arquivo {fileName}.")
            };

            processedRows = rows.Count;

            foreach (var row in rows)
            {
                try
                {
                    await _repository.CreateImportRowAsync(batchId, row, cancellationToken);
                    importedRows++;
                }
                catch (Exception ex)
                {
                    erroredRows++;
                    _logger.LogError(ex, "Falha ao inserir linha {RowNumber} do arquivo {FileName}", row.SourceRowNumber, fileName);
                }
            }

            finalStatus = erroredRows > 0 ? "processed_with_errors" : "processed";
            finalNote = $"Arquivo processado. Linhas lidas: {processedRows}. Importadas: {importedRows}. Erros: {erroredRows}.";

            await _repository.SetBatchStatusAsync(batchId, finalStatus, finalNote, cancellationToken);
            await _repository.FinishProcessingRunAsync(runId, finalStatus, processedRows, importedRows, erroredRows, finalNote, cancellationToken);

            MoveFileToProcessed(filePath);
            _logger.LogInformation("Arquivo {FileName} finalizado com status {Status}", fileName, finalStatus);
        }
        catch (Exception ex)
        {
            finalStatus = "failed";
            finalNote = ex.Message;

            _logger.LogError(ex, "Falha operacional ao processar o arquivo {FileName}", fileName);

            if (batchId != Guid.Empty)
            {
                await SafeSetBatchStatusAsync(batchId, finalStatus, finalNote, cancellationToken);
            }

            if (runId != Guid.Empty)
            {
                await SafeFinishRunAsync(runId, finalStatus, processedRows, importedRows, erroredRows + 1, finalNote, cancellationToken);
            }

            MoveFileToError(filePath);
        }
    }

    private (int? ReferenceYear, int? ReferenceMonth) ResolveReference(XLWorkbook workbook, string filePath, string parserType)
    {
        int? year = null;
        int? month = null;

        if (parserType == "hotel_agenda")
        {
            year = 2026;
        }
        else if (parserType == "creche_mensal")
        {
            year = TryResolveYear(filePath, workbook) ?? DateTime.UtcNow.Year;

            var monthlySheet = workbook.Worksheets.FirstOrDefault(x => MonthNameResolver.IsMonthlySheet(x.Name));
            if (monthlySheet is not null && MonthNameResolver.TryGetMonth(monthlySheet.Name, out var detectedMonth))
            {
                month = detectedMonth;
            }
        }

        return (year, month);
    }

    private string ResolveParserType(XLWorkbook workbook)
    {
        if (_hotelParser.CanHandle(workbook))
            return "hotel_agenda";

        if (_crecheParser.CanHandle(workbook))
            return "creche_mensal";

        throw new InvalidOperationException("Arquivo nao reconhecido como planilha de hotel ou creche.");
    }

    private bool IsSupportedFile(string path)
    {
        var ext = Path.GetExtension(path);

        if (ext.Equals(".xlsx", StringComparison.OrdinalIgnoreCase))
            return true;

        if (_options.IncludeCsv && ext.Equals(".csv", StringComparison.OrdinalIgnoreCase))
            return true;

        return false;
    }

    private void EnsureFolders()
    {
        Directory.CreateDirectory(_options.InputFolder);
        Directory.CreateDirectory(_options.ProcessedFolder);
        Directory.CreateDirectory(_options.ErrorFolder);
    }

    private void MoveFileToProcessed(string path)
    {
        var target = BuildUniqueTargetPath(_options.ProcessedFolder, Path.GetFileName(path));
        File.Move(path, target, false);
    }

    private void MoveFileToError(string path)
    {
        var target = BuildUniqueTargetPath(_options.ErrorFolder, Path.GetFileName(path));
        File.Move(path, target, false);
    }

    private static string BuildUniqueTargetPath(string folder, string fileName)
    {
        var target = Path.Combine(folder, fileName);
        if (!File.Exists(target))
            return target;

        var name = Path.GetFileNameWithoutExtension(fileName);
        var ext = Path.GetExtension(fileName);
        var stamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
        return Path.Combine(folder, $"{name}_{stamp}{ext}");
    }

    private static int? TryResolveYear(string filePath, XLWorkbook workbook)
    {
        var fromFile = SpreadsheetValueParser.ParseYear(Path.GetFileNameWithoutExtension(filePath));
        if (fromFile.HasValue)
            return fromFile.Value;

        foreach (var ws in workbook.Worksheets)
        {
            var fromSheet = SpreadsheetValueParser.ParseYear(ws.Name);
            if (fromSheet.HasValue)
                return fromSheet.Value;
        }

        return null;
    }

    private async Task SafeAddRowErrorAsync(
    long importRowId,
    string errorCode,
    string errorMessage,
    string errorStage,
    CancellationToken cancellationToken)
    {
        try
        {
            await _repository.AddRowErrorAsync(
                importRowId,
                errorCode,
                errorMessage,
                errorStage,
                cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Nao foi possivel registrar erro da row em dbo.usp_etl_import_row_error_add.");
        }
    }

    private async Task SafeSetBatchStatusAsync(
        Guid batchId,
        string status,
        string? note,
        CancellationToken cancellationToken)
    {
        try
        {
            await _repository.SetBatchStatusAsync(batchId, status, note, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Nao foi possivel atualizar o status do batch.");
        }
    }

    private async Task SafeFinishRunAsync(
        Guid runId,
        string status,
        int processedRows,
        int successRows,
        int errorRows,
        string? message,
        CancellationToken cancellationToken)
    {
        try
        {
            await _repository.FinishProcessingRunAsync(runId, status, processedRows, successRows, errorRows, message, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Nao foi possivel finalizar o processing run.");
        }
    }
}