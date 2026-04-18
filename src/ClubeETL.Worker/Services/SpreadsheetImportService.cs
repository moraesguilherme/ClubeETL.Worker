using ClubeETL.Worker.Configuration;
using ClubeETL.Worker.Models;
using ClubeETL.Worker.Parsing;
using ClubeETL.Worker.Persistence;
using ClubeETL.Worker.Utils;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ClubeETL.Worker.Services;

public sealed class SpreadsheetImportService : ISpreadsheetImportService
{
    private readonly UnifiedPaymentsWorkbookParser _parser;
    private readonly IEtlRepository _repository;
    private readonly ILogger<SpreadsheetImportService> _logger;
    private readonly EtlOptions _options;

    public SpreadsheetImportService(
        UnifiedPaymentsWorkbookParser parser,
        IEtlRepository repository,
        IOptions<EtlOptions> options,
        ILogger<SpreadsheetImportService> logger)
    {
        _parser = parser;
        _repository = repository;
        _logger = logger;
        _options = options.Value;
    }

    public async Task ProcessFileAsync(string filePath, CancellationToken cancellationToken)
    {
        var fileName = Path.GetFileName(filePath);

        Guid batchId = Guid.Empty;
        Guid processingRunId = Guid.Empty;

        var processedRows = 0;
        var successRows = 0;
        var ignoredRows = 0;
        var errorRows = 0;

        try
        {
            _logger.LogInformation("Iniciando processamento do arquivo {FileName}", fileName);

            var parsedRows = _parser.Parse(filePath);

            var referenceYear = parsedRows
                .Select(x => x.ReferenceYear)
                .FirstOrDefault(x => x.HasValue);

            var referenceMonth = parsedRows
                .Select(x => x.ReferenceMonth)
                .FirstOrDefault(x => x.HasValue);

            batchId = await _repository.CreateBatchAsync(
                fileName,
                filePath,
                "unified_payments_sheet",
                _options.SourceSystem,
                referenceYear,
                referenceMonth,
                cancellationToken);

            await _repository.SetBatchStatusAsync(
                batchId,
                "processing",
                "Arquivo em processamento.",
                cancellationToken);

            processingRunId = await _repository.CreateProcessingRunAsync(
                batchId,
                fileName,
                cancellationToken);

            foreach (var rawRow in parsedRows)
            {
                cancellationToken.ThrowIfCancellationRequested();
                processedRows++;

                try
                {
                    var state = await _repository.ResolveImportRowStateAsync(
                        rawRow.ExternalRowKey,
                        rawRow.SourceContentHash,
                        cancellationToken);

                    if (state.IsUnchanged)
                    {
                        ignoredRows++;

                        _logger.LogDebug(
                            "Linha ignorada por hash inalterado. ExternalRowKey={ExternalRowKey}",
                            rawRow.ExternalRowKey);

                        continue;
                    }

                    var pets = PetSplitter.Split(rawRow.RawPetNames);

                    if (pets.Count == 0)
                    {
                        throw new InvalidOperationException(
                            $"A linha {rawRow.SourceRowNumber} nao possui pets validos para importacao.");
                    }

                    var insertedImportRowIds = new List<long>();

                    for (var i = 0; i < pets.Count; i++)
                    {
                        var importRow = MapToImportRow(rawRow, pets[i], i + 1);

                        var importRowId = await _repository.CreateImportRowAsync(
                            batchId,
                            importRow,
                            cancellationToken);

                        insertedImportRowIds.Add(importRowId);

                        await _repository.SetImportRowStatusAsync(
                            importRowId,
                            "processed",
                            cancellationToken);
                    }

                    if (state.IsChanged &&
                        state.ExistingImportRowId.HasValue &&
                        insertedImportRowIds.Count > 0)
                    {
                        await _repository.SupersedeImportRowAsync(
                            state.ExistingImportRowId.Value,
                            insertedImportRowIds[0],
                            cancellationToken);
                    }

                    successRows += insertedImportRowIds.Count;
                }
                catch (Exception rowEx)
                {
                    errorRows++;

                    _logger.LogError(
                        rowEx,
                        "Erro ao processar linha {RowNumber} do arquivo {FileName}",
                        rawRow.SourceRowNumber,
                        fileName);
                }
            }

            var finalBatchStatus = errorRows > 0
                ? "processed_with_errors"
                : "processed";

            var runSummary =
                $"processed_rows={processedRows}; success_rows={successRows}; ignored_rows={ignoredRows}; error_rows={errorRows}";

            await _repository.FinishProcessingRunAsync(
                processingRunId,
                finalBatchStatus,
                processedRows,
                successRows,
                errorRows,
                runSummary,
                cancellationToken);

            await _repository.SetBatchStatusAsync(
                batchId,
                finalBatchStatus,
                runSummary,
                cancellationToken);

            MoveToProcessedFolder(filePath);

            _logger.LogInformation(
                "Processamento concluido. Arquivo={FileName}; Processadas={ProcessedRows}; Sucesso={SuccessRows}; Ignoradas={IgnoredRows}; Erros={ErrorRows}",
                fileName,
                processedRows,
                successRows,
                ignoredRows,
                errorRows);
        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning(
                "Processamento cancelado para o arquivo {FileName}",
                fileName);

            if (processingRunId != Guid.Empty)
            {
                await SafeFinishRunAsync(
                    processingRunId,
                    "cancelled",
                    processedRows,
                    successRows,
                    errorRows,
                    "Processamento cancelado.");
            }

            if (batchId != Guid.Empty)
            {
                await SafeSetBatchStatusAsync(
                    batchId,
                    "cancelled",
                    "Processamento cancelado.");
            }

            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Falha operacional ao processar o arquivo {FileName}",
                fileName);

            if (processingRunId != Guid.Empty)
            {
                await SafeFinishRunAsync(
                    processingRunId,
                    "failed",
                    processedRows,
                    successRows,
                    errorRows,
                    ex.Message);
            }

            if (batchId != Guid.Empty)
            {
                await SafeSetBatchStatusAsync(
                    batchId,
                    "failed",
                    ex.Message);
            }

            MoveToErrorFolder(filePath);

            throw;
        }
    }

    private static ImportRowModel MapToImportRow(
        UnifiedPaymentRawRow rawRow,
        string petName,
        int petSplitIndex)
    {
        return new ImportRowModel
        {
            RowNumber = rawRow.SourceRowNumber,
            ExternalRowKey = rawRow.ExternalRowKey,
            SourceContentHash = rawRow.SourceContentHash,
            RawPayloadJson = rawRow.RawPayloadJson,

            SourceFileName = rawRow.SourceFileName,
            SourceSheetName = rawRow.SourceSheetName,
            SourceFileType = rawRow.SourceFileType,

            OccurredAt = rawRow.OccurredAt,
            CompetenceDate = rawRow.CompetenceDate,

            CustomerNameRaw = rawRow.CustomerNameRaw,
            CustomerDocumentRaw = rawRow.CustomerDocumentRaw,
            CustomerPhoneRaw = rawRow.CustomerPhoneRaw,

            RawPetNames = rawRow.RawPetNames,
            PetNameRaw = petName,
            PetSplitIndex = petSplitIndex,

            ServiceTypeRaw = rawRow.ServiceTypeRaw,
            PlanNameRaw = rawRow.PlanNameRaw,
            PackageNameRaw = rawRow.PackageNameRaw,

            PaymentMethodRaw = rawRow.PaymentMethodRaw,
            PaymentStatusRaw = rawRow.PaymentStatusRaw,

            GrossAmount = rawRow.GrossAmount,
            TaxiAmount = rawRow.TaxiAmount,
            GroupTotalAmount = rawRow.GroupTotalAmount,
            NetAmount = rawRow.NetAmount,

            StartDate = rawRow.StartDate,
            EndDate = rawRow.EndDate,

            ObservationRaw = rawRow.ObservationRaw,

            ReferenceYear = rawRow.ReferenceYear,
            ReferenceMonth = rawRow.ReferenceMonth
        };
    }

    private void MoveToProcessedFolder(string filePath)
    {
        if (string.IsNullOrWhiteSpace(_options.ProcessedFolder))
        {
            return;
        }

        Directory.CreateDirectory(_options.ProcessedFolder);

        var destinationPath = BuildDestinationPath(_options.ProcessedFolder, filePath);

        if (File.Exists(destinationPath))
        {
            File.Delete(destinationPath);
        }

        File.Move(filePath, destinationPath);
    }

    private void MoveToErrorFolder(string filePath)
    {
        if (string.IsNullOrWhiteSpace(_options.ErrorFolder))
        {
            return;
        }

        if (!File.Exists(filePath))
        {
            return;
        }

        Directory.CreateDirectory(_options.ErrorFolder);

        var destinationPath = BuildDestinationPath(_options.ErrorFolder, filePath);

        if (File.Exists(destinationPath))
        {
            File.Delete(destinationPath);
        }

        File.Move(filePath, destinationPath);
    }

    private static string BuildDestinationPath(string folderPath, string originalFilePath)
    {
        var fileName = Path.GetFileName(originalFilePath);
        return Path.Combine(folderPath, fileName);
    }

    private async Task SafeFinishRunAsync(
        Guid processingRunId,
        string status,
        int processedRows,
        int successRows,
        int errorRows,
        string? message)
    {
        try
        {
            await _repository.FinishProcessingRunAsync(
                processingRunId,
                status,
                processedRows,
                successRows,
                errorRows,
                message,
                CancellationToken.None);
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Falha ao finalizar processing run {ProcessingRunId} com status {Status}",
                processingRunId,
                status);
        }
    }

    private async Task SafeSetBatchStatusAsync(
        Guid batchId,
        string status,
        string? note)
    {
        try
        {
            await _repository.SetBatchStatusAsync(
                batchId,
                status,
                note,
                CancellationToken.None);
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Falha ao atualizar batch {BatchId} com status {Status}",
                batchId,
                status);
        }
    }
}