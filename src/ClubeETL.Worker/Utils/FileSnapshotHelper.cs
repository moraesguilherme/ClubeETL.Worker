namespace ClubeETL.Worker.Utils;

public static class FileSnapshotHelper
{
    public static bool IsTemporaryOfficeFile(string filePath)
    {
        var fileName = Path.GetFileName(filePath);
        return fileName.StartsWith("~$", StringComparison.OrdinalIgnoreCase);
    }

    public static string CreateSnapshotCopy(string originalFilePath)
    {
        var snapshotFolder = Path.Combine(AppContext.BaseDirectory, "data", "snapshots");
        Directory.CreateDirectory(snapshotFolder);

        var fileNameWithoutExtension = Path.GetFileNameWithoutExtension(originalFilePath);
        var extension = Path.GetExtension(originalFilePath);
        var timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss_fff");

        var snapshotPath = Path.Combine(
            snapshotFolder,
            $"{fileNameWithoutExtension}_{timestamp}{extension}");

        File.Copy(originalFilePath, snapshotPath, overwrite: false);

        return snapshotPath;
    }

    public static void TryDeleteSnapshot(string? snapshotPath)
    {
        if (string.IsNullOrWhiteSpace(snapshotPath))
        {
            return;
        }

        try
        {
            if (File.Exists(snapshotPath))
            {
                File.Delete(snapshotPath);
            }
        }
        catch
        {
        }
    }
}