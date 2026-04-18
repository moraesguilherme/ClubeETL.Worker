namespace ClubeETL.Worker.Utils;

public static class FileAccessHelper
{
    public static bool CanRead(string filePath)
    {
        try
        {
            using var stream = new FileStream(
                filePath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.ReadWrite);

            return stream.Length >= 0;
        }
        catch
        {
            return false;
        }
    }
}