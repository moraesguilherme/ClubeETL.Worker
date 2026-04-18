namespace ClubeETL.Worker.Models;

public sealed class FileProcessingFingerprint
{
    public string FilePath { get; set; } = string.Empty;
    public long Length { get; set; }
    public DateTime LastWriteTimeUtc { get; set; }

    public static FileProcessingFingerprint FromFile(string filePath)
    {
        var fileInfo = new FileInfo(filePath);

        return new FileProcessingFingerprint
        {
            FilePath = Path.GetFullPath(filePath),
            Length = fileInfo.Length,
            LastWriteTimeUtc = fileInfo.LastWriteTimeUtc
        };
    }

    public string ToStableKey()
    {
        return $"{FilePath}|{Length}|{LastWriteTimeUtc.Ticks}";
    }
}