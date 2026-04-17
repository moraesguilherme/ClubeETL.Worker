using ClosedXML.Excel;

namespace ClubeETL.Worker.Parsing;

public static class WorksheetRowReader
{
    public static Dictionary<string, object?> ReadRow(IXLWorksheet ws, int rowNumber, int lastColumn)
    {
        var dict = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

        for (var col = 1; col <= lastColumn; col++)
        {
            dict[$"C{col}"] = ws.Cell(rowNumber, col).Value.ToString();
        }

        return dict;
    }
}