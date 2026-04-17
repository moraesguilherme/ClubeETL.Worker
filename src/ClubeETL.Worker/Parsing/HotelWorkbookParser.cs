using System.Text.Json;
using ClosedXML.Excel;
using ClubeETL.Worker.Models;
using ClubeETL.Worker.Utils;

namespace ClubeETL.Worker.Parsing;

public sealed class HotelWorkbookParser
{
    private const string WorksheetName = "AGENDA 2026";
    private const int HeaderRow = 2;
    private const int StartRow = 3;

    public bool CanHandle(XLWorkbook workbook)
        => workbook.Worksheets.Any(w => w.Name.Equals(WorksheetName, StringComparison.OrdinalIgnoreCase));

    public IReadOnlyList<ImportRowModel> Parse(string filePath, XLWorkbook workbook)
    {
        var ws = workbook.Worksheet(WorksheetName);
        var lastRow = ws.LastRowUsed()?.RowNumber() ?? 0;
        var lastColumn = ws.LastColumnUsed()?.ColumnNumber() ?? 0;

        if (lastRow < StartRow || lastColumn == 0)
            return Array.Empty<ImportRowModel>();

        var headers = BuildHeaderMap(ws, HeaderRow, lastColumn);
        var referenceYear = SpreadsheetValueParser.ParseYear(WorksheetName) ?? DateTime.UtcNow.Year;

        var items = new List<ImportRowModel>();

        for (var row = StartRow; row <= lastRow; row++)
        {
            var rowPayload = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            for (var col = 1; col <= lastColumn; col++)
            {
                var header = ws.Cell(HeaderRow, col).GetString().Trim();
                var key = string.IsNullOrWhiteSpace(header) ? $"C{col}" : header;
                rowPayload[key] = ws.Cell(row, col).Value.ToString();
            }

            var petName = GetValue(ws, row, headers, "CACHORRO");
            var tutor = GetValue(ws, row, headers, "TUTOR");
            var cpf = GetValue(ws, row, headers, "CPF");
            var phone = GetValue(ws, row, headers, "TELEFONE");
            var pgto = GetValue(ws, row, headers, "PGTO");
            var periodo = GetValue(ws, row, headers, "PERIODO");
            var inicial = GetValue(ws, row, headers, "INICIAL");
            var final = GetValue(ws, row, headers, "FINAL");
            var valor = GetValue(ws, row, headers, "VALOR");

            if (IsRowEmpty(petName, tutor, valor, inicial, final))
                continue;

            var startDate = SpreadsheetValueParser.ParseDate(inicial);
            var endDate = SpreadsheetValueParser.ParseDate(final);
            var amount = SpreadsheetValueParser.ParseDecimal(valor);

            var competenceDate = startDate.HasValue
                ? new DateTime(startDate.Value.Year, startDate.Value.Month, 1)
                : new DateTime(referenceYear, 1, 1);

            items.Add(new ImportRowModel
            {
                SourceRowNumber = row,
                CustomerNameRaw = SpreadsheetValueParser.CleanString(tutor),
                CustomerDocumentRaw = SpreadsheetValueParser.CleanString(cpf),
                CustomerPhoneRaw = SpreadsheetValueParser.CleanString(phone),
                PetNameRaw = SpreadsheetValueParser.CleanString(petName),
                ServiceTypeRaw = "hotel",
                LodgingTypeRaw = SpreadsheetValueParser.CleanString(periodo),
                GrossAmount = amount,
                NetAmount = amount,
                StartDate = startDate,
                EndDate = endDate,
                OccurredAt = startDate ?? endDate,
                CompetenceDate = competenceDate,
                PaymentStatusRaw = SpreadsheetValueParser.CleanString(pgto),
                PaymentMethodRaw = null,
                DescriptionRaw = null,
                ObservationRaw = null,
                SourceSheetName = WorksheetName,
                SourceFileType = "hotel_agenda",
                ReferenceYear = referenceYear,
                RawPayloadJson = JsonSerializer.Serialize(rowPayload)
            });
        }

        return items;
    }

    private static Dictionary<string, int> BuildHeaderMap(IXLWorksheet ws, int headerRow, int lastColumn)
    {
        var map = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

        for (var col = 1; col <= lastColumn; col++)
        {
            var raw = ws.Cell(headerRow, col).GetString().Trim();
            if (string.IsNullOrWhiteSpace(raw))
                continue;

            var normalized = TextNormalizer.NormalizeKey(raw);
            if (!map.ContainsKey(normalized))
                map[normalized] = col;
        }

        return map;
    }

    private static object? GetValue(IXLWorksheet ws, int row, Dictionary<string, int> headers, string headerAlias)
    {
        var key = TextNormalizer.NormalizeKey(headerAlias);
        if (!headers.TryGetValue(key, out var column))
            return null;

        return ws.Cell(row, column).Value.ToString();
    }

    private static bool IsRowEmpty(params object?[] values)
        => values.All(v => string.IsNullOrWhiteSpace(Convert.ToString(v)));
}