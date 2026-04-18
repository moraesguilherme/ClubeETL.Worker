using System.IO;

namespace ClubeETL.Worker.Utils;

public static class UnifiedPaymentsRowKeyBuilder
{
    public static string Build(
        string sourceFileName,
        string sourceSheetName,
        int sourceRowNumber)
    {
        var fileName = Path.GetFileName(sourceFileName)?.Trim() ?? string.Empty;
        var sheetName = sourceSheetName?.Trim() ?? string.Empty;

        return $"{fileName}|{sheetName}|{sourceRowNumber}";
    }
}