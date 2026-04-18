using ClubeETL.Worker.Models;

namespace ClubeETL.Worker.Services;

public interface IProcessedFileStateStore
{
    bool HasSameSuccessfulFingerprint(FileProcessingFingerprint fingerprint);
    void MarkAsSuccessfullyProcessed(FileProcessingFingerprint fingerprint);
    Task SaveAsync(CancellationToken cancellationToken);
}