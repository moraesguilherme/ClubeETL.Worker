namespace ClubeETL.Worker.Services;

public interface ISpreadsheetImportService
{
    Task ImportPendingFilesAsync(CancellationToken cancellationToken);
}