using ClubeETL.Worker.Models;

namespace ClubeETL.Worker.Persistence;

public interface IEtlRepository
{
    Task<Guid> CreateBatchAsync(
        string fileName,
        string filePath,
        string sourceFileType,
        string sourceSystem,
        int? referenceYear,
        int? referenceMonth,
        CancellationToken cancellationToken);

    Task SetBatchStatusAsync(
        Guid batchId,
        string status,
        string? note,
        CancellationToken cancellationToken);

    Task<Guid> CreateProcessingRunAsync(
        Guid batchId,
        string fileName,
        CancellationToken cancellationToken);

    Task FinishProcessingRunAsync(
        Guid processingRunId,
        string status,
        int processedRows,
        int successRows,
        int errorRows,
        string? message,
        CancellationToken cancellationToken);

    Task<ImportRowStateResolution> ResolveImportRowStateAsync(
        string externalRowKey,
        string sourceContentHash,
        CancellationToken cancellationToken);

    Task<long> CreateImportRowAsync(
        Guid batchId,
        ImportRowModel row,
        CancellationToken cancellationToken);

    Task SetImportRowStatusAsync(
        long importRowId,
        string status,
        CancellationToken cancellationToken);

    Task AddRowErrorAsync(
        long importRowId,
        string errorCode,
        string errorMessage,
        string errorStage,
        CancellationToken cancellationToken);

    Task SupersedeCurrentRowsByExternalRowKeyAsync(
        string externalRowKey,
        long? replacedByImportRowId,
        CancellationToken cancellationToken);

    Task SetReplacementForSupersededRowsAsync(
    string externalRowKey,
    long replacedByImportRowId,
    CancellationToken cancellationToken);
}