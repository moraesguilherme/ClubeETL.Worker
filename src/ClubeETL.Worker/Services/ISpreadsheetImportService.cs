namespace ClubeETL.Worker.Services;

public interface ISpreadsheetImportService
{
    Task ProcessFileAsync(string filePath, CancellationToken cancellationToken);
}