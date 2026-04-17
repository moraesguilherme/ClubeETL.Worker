namespace ClubeETL.Worker.Models;

public sealed class BatchCreateResult
{
    public Guid BatchId { get; set; }
}

public sealed class ProcessingRunCreateResult
{
    public Guid ProcessingRunId { get; set; }
}

public sealed class FileProcessingSummary
{
    public int RowsRead { get; set; }
    public int RowsImported { get; set; }
    public int RowsErrored { get; set; }
    public string FinalStatus { get; set; } = "completed";
    public string? Note { get; set; }
}

public sealed class ImportRowCreatedResult
{
    public long Id { get; set; }
}