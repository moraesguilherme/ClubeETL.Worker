using System.Data;
using ClubeETL.Worker.Models;
using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;

namespace ClubeETL.Worker.Persistence;

public sealed class EtlRepository : IEtlRepository
{
    private readonly IConfiguration _configuration;

    public EtlRepository(IConfiguration configuration)
    {
        _configuration = configuration;
    }

    private SqlConnection CreateConnection()
    {
        var connectionString = _configuration.GetConnectionString("DefaultConnection");

        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new InvalidOperationException("ConnectionStrings:DefaultConnection nao configurada.");
        }

        return new SqlConnection(connectionString);
    }

    public async Task<Guid> CreateBatchAsync(
        string fileName,
        string filePath,
        string sourceFileType,
        string sourceSystem,
        int? referenceYear,
        int? referenceMonth,
        CancellationToken cancellationToken)
    {
        const string sql = "dbo.usp_etl_import_batch_create";

        using var connection = CreateConnection();
        await connection.OpenAsync(cancellationToken);

        var batchId = Guid.NewGuid();

        var parameters = new DynamicParameters();
        parameters.Add("@Id", batchId, DbType.Guid);
        parameters.Add("@SourceName", sourceSystem, DbType.String);
        parameters.Add("@SourceType", "spreadsheet", DbType.String);
        parameters.Add("@FileName", fileName, DbType.String);
        parameters.Add("@FileHash", null, DbType.String);
        parameters.Add("@StartedAt", DateTime.UtcNow, DbType.DateTime2);
        parameters.Add("@CreatedByUserId", null, DbType.Guid);
        parameters.Add(
            "@Notes",
            BuildBatchNotes(filePath, sourceFileType, referenceYear, referenceMonth),
            DbType.String);

        await connection.ExecuteAsync(
            new CommandDefinition(
                sql,
                parameters,
                commandType: CommandType.StoredProcedure,
                cancellationToken: cancellationToken));

        return batchId;
    }

    public async Task SetBatchStatusAsync(
        Guid batchId,
        string status,
        string? note,
        CancellationToken cancellationToken)
    {
        const string sql = "dbo.usp_etl_import_batch_set_status";

        using var connection = CreateConnection();
        await connection.OpenAsync(cancellationToken);

        var parameters = new DynamicParameters();
        parameters.Add("@BatchId", batchId, DbType.Guid);
        parameters.Add("@Status", status, DbType.String);
        parameters.Add("@FinishedAt", DateTime.UtcNow, DbType.DateTime2);
        parameters.Add("@Notes", note, DbType.String);

        await connection.ExecuteAsync(
            new CommandDefinition(
                sql,
                parameters,
                commandType: CommandType.StoredProcedure,
                cancellationToken: cancellationToken));
    }

    public async Task<Guid> CreateProcessingRunAsync(
        Guid batchId,
        string fileName,
        CancellationToken cancellationToken)
    {
        const string sql = "dbo.usp_etl_processing_run_create";

        using var connection = CreateConnection();
        await connection.OpenAsync(cancellationToken);

        var processingRunId = Guid.NewGuid();

        var parameters = new DynamicParameters();
        parameters.Add("@Id", processingRunId, DbType.Guid);
        parameters.Add("@BatchId", batchId, DbType.Guid);
        parameters.Add("@RunType", "import", DbType.String);
        parameters.Add("@StartedAt", DateTime.UtcNow, DbType.DateTime2);

        await connection.ExecuteAsync(
            new CommandDefinition(
                sql,
                parameters,
                commandType: CommandType.StoredProcedure,
                cancellationToken: cancellationToken));

        return processingRunId;
    }

    public async Task FinishProcessingRunAsync(
        Guid processingRunId,
        string status,
        int processedRows,
        int successRows,
        int errorRows,
        string? message,
        CancellationToken cancellationToken)
    {
        const string sql = "dbo.usp_etl_processing_run_finish";

        using var connection = CreateConnection();
        await connection.OpenAsync(cancellationToken);

        var parameters = new DynamicParameters();
        parameters.Add("@Id", processingRunId, DbType.Guid);
        parameters.Add("@Status", status, DbType.String);
        parameters.Add("@ProcessedItems", processedRows, DbType.Int32);
        parameters.Add("@SuccessItems", successRows, DbType.Int32);
        parameters.Add("@ErrorItems", errorRows, DbType.Int32);
        parameters.Add("@LogSummary", message, DbType.String);
        parameters.Add("@FinishedAt", DateTime.UtcNow, DbType.DateTime2);

        await connection.ExecuteAsync(
            new CommandDefinition(
                sql,
                parameters,
                commandType: CommandType.StoredProcedure,
                cancellationToken: cancellationToken));
    }

    public async Task<ImportRowStateResolution> ResolveImportRowStateAsync(
        string externalRowKey,
        string sourceContentHash,
        CancellationToken cancellationToken)
    {
        const string sql = "dbo.usp_etl_import_row_resolve_state";

        using var connection = CreateConnection();
        await connection.OpenAsync(cancellationToken);

        var parameters = new DynamicParameters();
        parameters.Add("@ExternalRowKey", externalRowKey, DbType.String);
        parameters.Add("@SourceContentHash", sourceContentHash, DbType.String);

        var result = await connection.QuerySingleAsync<ResolveImportRowStateDbResult>(
            new CommandDefinition(
                sql,
                parameters,
                commandType: CommandType.StoredProcedure,
                cancellationToken: cancellationToken));

        return new ImportRowStateResolution
        {
            State = result.RowState ?? string.Empty,
            ExistingImportRowId = result.ExistingImportRowId
        };
    }

    public async Task<long> CreateImportRowAsync(
        Guid batchId,
        ImportRowModel row,
        CancellationToken cancellationToken)
    {
        const string sql = "dbo.usp_etl_import_row_create";

        using var connection = CreateConnection();
        await connection.OpenAsync(cancellationToken);

        var parameters = new DynamicParameters();
        parameters.Add("@BatchId", batchId, DbType.Guid);
        parameters.Add("@RowNumber", row.RowNumber, DbType.Int32);
        parameters.Add("@ExternalRowKey", row.ExternalRowKey, DbType.String);
        parameters.Add("@SourceContentHash", row.SourceContentHash, DbType.String);
        parameters.Add("@RawPayloadJson", row.RawPayloadJson, DbType.String);

        parameters.Add("@SourceFileName", row.SourceFileName, DbType.String);
        parameters.Add("@SourceSheetName", row.SourceSheetName, DbType.String);
        parameters.Add("@SourceFileType", row.SourceFileType, DbType.String);

        parameters.Add("@OccurredAt", row.OccurredAt, DbType.DateTime2);
        parameters.Add("@CompetenceDate", row.CompetenceDate?.Date, DbType.Date);

        parameters.Add("@CustomerNameRaw", row.CustomerNameRaw, DbType.String);
        parameters.Add("@CustomerDocumentRaw", row.CustomerDocumentRaw, DbType.String);
        parameters.Add("@CustomerPhoneRaw", row.CustomerPhoneRaw, DbType.String);

        parameters.Add("@RawPetNames", row.RawPetNames, DbType.String);
        parameters.Add("@PetNameRaw", row.PetNameRaw, DbType.String);
        parameters.Add("@PetSplitIndex", row.PetSplitIndex, DbType.Int32);

        parameters.Add("@ServiceTypeRaw", row.ServiceTypeRaw, DbType.String);
        parameters.Add("@PlanNameRaw", row.PlanNameRaw, DbType.String);
        parameters.Add("@PackageNameRaw", row.PackageNameRaw, DbType.String);

        parameters.Add("@PaymentMethodRaw", row.PaymentMethodRaw, DbType.String);
        parameters.Add("@PaymentStatusRaw", row.PaymentStatusRaw, DbType.String);

        parameters.Add("@GrossAmount", row.GrossAmount, DbType.Decimal);
        parameters.Add("@TaxiAmount", row.TaxiAmount, DbType.Decimal);
        parameters.Add("@GroupTotalAmount", row.GroupTotalAmount, DbType.Decimal);

        parameters.Add("@StartDate", row.StartDate?.Date, DbType.Date);
        parameters.Add("@EndDate", row.EndDate?.Date, DbType.Date);

        parameters.Add("@ObservationRaw", row.ObservationRaw, DbType.String);

        parameters.Add("@ReferenceYear", row.ReferenceYear, DbType.Int32);
        parameters.Add("@ReferenceMonth", row.ReferenceMonth, DbType.Int32);

        var inserted = await connection.QuerySingleAsync<InsertedImportRowDbResult>(
            new CommandDefinition(
                sql,
                parameters,
                commandType: CommandType.StoredProcedure,
                cancellationToken: cancellationToken));

        return inserted.Id;
    }

    public async Task SupersedeImportRowAsync(
        long importRowId,
        long replacedByImportRowId,
        CancellationToken cancellationToken)
    {
        const string sql = "dbo.usp_etl_import_row_supersede";

        using var connection = CreateConnection();
        await connection.OpenAsync(cancellationToken);

        var parameters = new DynamicParameters();
        parameters.Add("@ExistingImportRowId", importRowId, DbType.Int64);
        parameters.Add("@ReplacedByImportRowId", replacedByImportRowId, DbType.Int64);

        await connection.ExecuteAsync(
            new CommandDefinition(
                sql,
                parameters,
                commandType: CommandType.StoredProcedure,
                cancellationToken: cancellationToken));
    }

    public async Task SetImportRowStatusAsync(
        long importRowId,
        string status,
        CancellationToken cancellationToken)
    {
        const string sql = "dbo.usp_etl_import_row_set_status";

        using var connection = CreateConnection();
        await connection.OpenAsync(cancellationToken);

        var parameters = new DynamicParameters();
        parameters.Add("@ImportRowId", importRowId, DbType.Int64);
        parameters.Add("@Status", status, DbType.String);

        await connection.ExecuteAsync(
            new CommandDefinition(
                sql,
                parameters,
                commandType: CommandType.StoredProcedure,
                cancellationToken: cancellationToken));
    }

    public async Task AddRowErrorAsync(
        long importRowId,
        string errorCode,
        string errorMessage,
        string errorStage,
        CancellationToken cancellationToken)
    {
        const string sql = "dbo.usp_etl_import_row_error_add";

        using var connection = CreateConnection();
        await connection.OpenAsync(cancellationToken);

        var parameters = new DynamicParameters();
        parameters.Add("@ImportRowId", importRowId, DbType.Int64);
        parameters.Add("@ErrorCode", errorCode, DbType.String);
        parameters.Add("@ErrorMessage", errorMessage, DbType.String);
        parameters.Add("@ErrorStage", errorStage, DbType.String);

        await connection.ExecuteAsync(
            new CommandDefinition(
                sql,
                parameters,
                commandType: CommandType.StoredProcedure,
                cancellationToken: cancellationToken));
    }

    private static string? BuildBatchNotes(
        string filePath,
        string sourceFileType,
        int? referenceYear,
        int? referenceMonth)
    {
        var parts = new List<string>();

        if (!string.IsNullOrWhiteSpace(filePath))
        {
            parts.Add($"file_path={filePath}");
        }

        if (!string.IsNullOrWhiteSpace(sourceFileType))
        {
            parts.Add($"source_file_type={sourceFileType}");
        }

        if (referenceYear.HasValue)
        {
            parts.Add($"reference_year={referenceYear.Value}");
        }

        if (referenceMonth.HasValue)
        {
            parts.Add($"reference_month={referenceMonth.Value}");
        }

        return parts.Count == 0 ? null : string.Join("; ", parts);
    }

    private sealed class ResolveImportRowStateDbResult
    {
        public string? RowState { get; set; }
        public long? ExistingImportRowId { get; set; }
    }

    private sealed class InsertedImportRowDbResult
    {
        public long Id { get; set; }
    }
}