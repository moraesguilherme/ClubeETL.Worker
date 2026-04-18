namespace ClubeETL.Worker.Services;

public interface ISpreadsheetImportService
{
    Task ProcessFileAsync(string filePath, string originalFilePath, CancellationToken cancellationToken);
}