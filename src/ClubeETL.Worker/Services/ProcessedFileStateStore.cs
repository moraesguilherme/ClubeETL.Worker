using System.Text.Json;
using ClubeETL.Worker.Models;

namespace ClubeETL.Worker.Services;

public sealed class ProcessedFileStateStore : IProcessedFileStateStore
{
    private readonly string _stateFilePath;
    private readonly Dictionary<string, string> _successfulFingerprintsByFile;

    public ProcessedFileStateStore()
    {
        var stateFolder = Path.Combine(AppContext.BaseDirectory, "data", "state");
        Directory.CreateDirectory(stateFolder);

        _stateFilePath = Path.Combine(stateFolder, "processed-files.json");
        _successfulFingerprintsByFile = Load();
    }

    public bool HasSameSuccessfulFingerprint(FileProcessingFingerprint fingerprint)
    {
        var filePath = Path.GetFullPath(fingerprint.FilePath);
        var stableKey = fingerprint.ToStableKey();

        return _successfulFingerprintsByFile.TryGetValue(filePath, out var savedKey)
            && string.Equals(savedKey, stableKey, StringComparison.Ordinal);
    }

    public void MarkAsSuccessfullyProcessed(FileProcessingFingerprint fingerprint)
    {
        var filePath = Path.GetFullPath(fingerprint.FilePath);
        _successfulFingerprintsByFile[filePath] = fingerprint.ToStableKey();
    }

    public async Task SaveAsync(CancellationToken cancellationToken)
    {
        var json = JsonSerializer.Serialize(
            _successfulFingerprintsByFile,
            new JsonSerializerOptions { WriteIndented = true });

        await File.WriteAllTextAsync(_stateFilePath, json, cancellationToken);
    }

    private Dictionary<string, string> Load()
    {
        if (!File.Exists(_stateFilePath))
        {
            return new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        }

        try
        {
            var json = File.ReadAllText(_stateFilePath);
            var data = JsonSerializer.Deserialize<Dictionary<string, string>>(json);

            return data is null
                ? new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                : new Dictionary<string, string>(data, StringComparer.OrdinalIgnoreCase);
        }
        catch
        {
            return new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        }
    }
}