using System.Text.Json;
using ClosedXML.Excel;
using ClubeETL.Worker.Models;
using ClubeETL.Worker.Utils;

namespace ClubeETL.Worker.Parsing;

public sealed class CrecheWorkbookParser
{
    private static readonly string[] IgnoredSheets =
    {
        "LISTA APOIO",
        "BRINDES",
        "PLANOS HOTEL",
        "TURMA"
    };

    private static readonly Dictionary<string, string[]> HeaderAliases = new(StringComparer.OrdinalIgnoreCase)
    {
        ["STATUS"] = new[] { "STATUS" },
        ["TIPO"] = new[] { "TIPO" },
        ["CACHORRO"] = new[] { "CACHORRO", "PET", "NOME DO CACHORRO" },
        ["TUTOR"] = new[] { "TUTOR", "DONO", "RESPONSAVEL", "RESPONSAVEL LEGAL" },
        ["CPF"] = new[] { "CPF", "DOCUMENTO" },
        ["TELEFONE"] = new[] { "TELEFONE", "CELULAR", "WHATSAPP", "FONE" },
        ["DESCRICAO"] = new[] { "DESCRICAO", "DESCRICAO DO SERVICO" },
        ["VALOR"] = new[] { "VALOR" },
        ["TAXI"] = new[] { "TAXI", "TAXI DOG", "TAXI-DOG" },
        ["SOMA TOTAL"] = new[] { "SOMA TOTAL", "TOTAL", "TOTAL GERAL" },
        ["FORMA DE PAGAMENTO"] = new[] { "FORMA DE PAGAMENTO", "PAGAMENTO", "FORMA PGTO", "FORMA PAGTO" },
        ["OBSERVACAO"] = new[] { "OBSERVACAO", "OBS" }
    };

    public bool CanHandle(XLWorkbook workbook)
        => workbook.Worksheets.Any(IsRelevantMonthlyWorksheet);

    public IReadOnlyList<ImportRowModel> Parse(string filePath, XLWorkbook workbook)
    {
        var items = new List<ImportRowModel>();
        var referenceYear = TryGetReferenceYear(filePath, workbook);

        foreach (var ws in workbook.Worksheets.Where(IsRelevantMonthlyWorksheet))
        {
            var headerRow = DetectHeaderRow(ws);
            if (headerRow is null)
                continue;

            var lastRow = ws.LastRowUsed()?.RowNumber() ?? 0;
            var lastColumn = ws.LastColumnUsed()?.ColumnNumber() ?? 0;
            if (lastRow <= headerRow.Value || lastColumn == 0)
                continue;

            var map = BuildAliasMap(ws, headerRow.Value, lastColumn);

            int? month = null;
            if (MonthNameResolver.TryGetMonth(ws.Name, out var detectedMonth))
                month = detectedMonth;

            for (var row = headerRow.Value + 1; row <= lastRow; row++)
            {
                var payload = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
                for (var col = 1; col <= lastColumn; col++)
                {
                    var header = ws.Cell(headerRow.Value, col).GetString().Trim();
                    var key = string.IsNullOrWhiteSpace(header) ? $"C{col}" : header;
                    payload[key] = ws.Cell(row, col).Value.ToString();
                }

                var status = GetValue(ws, row, map, "STATUS");
                var tipo = GetValue(ws, row, map, "TIPO");
                var cachorro = GetValue(ws, row, map, "CACHORRO");
                var tutor = GetValue(ws, row, map, "TUTOR");
                var cpf = GetValue(ws, row, map, "CPF");
                var telefone = GetValue(ws, row, map, "TELEFONE");
                var descricao = GetValue(ws, row, map, "DESCRICAO");
                var valor = GetValue(ws, row, map, "VALOR");
                var taxi = GetValue(ws, row, map, "TAXI");
                var somaTotal = GetValue(ws, row, map, "SOMA TOTAL");
                var formaPagamento = GetValue(ws, row, map, "FORMA DE PAGAMENTO");
                var obs = GetValue(ws, row, map, "OBSERVACAO");

                if (IsRowEmpty(cachorro, tutor, descricao, valor, somaTotal, tipo))
                    continue;

                var gross = SpreadsheetValueParser.ParseDecimal(valor);
                var taxiAmount = SpreadsheetValueParser.ParseDecimal(taxi);
                var net = SpreadsheetValueParser.ParseDecimal(somaTotal) ?? gross;

                DateTime? competence = null;
                if (referenceYear.HasValue && month.HasValue)
                    competence = new DateTime(referenceYear.Value, month.Value, 1);

                items.Add(new ImportRowModel
                {
                    SourceRowNumber = row,
                    CustomerNameRaw = SpreadsheetValueParser.CleanString(tutor),
                    CustomerDocumentRaw = SpreadsheetValueParser.CleanString(cpf),
                    CustomerPhoneRaw = SpreadsheetValueParser.CleanString(telefone),
                    PetNameRaw = SpreadsheetValueParser.CleanString(cachorro),
                    ServiceTypeRaw = SpreadsheetValueParser.CleanString(tipo) ?? "creche",
                    LodgingTypeRaw = null,
                    PlanNameRaw = null,
                    PackageNameRaw = SpreadsheetValueParser.CleanString(tipo),
                    GrossAmount = gross,
                    TaxiAmount = taxiAmount,
                    NetAmount = net,
                    StartDate = null,
                    EndDate = null,
                    OccurredAt = competence,
                    CompetenceDate = competence,
                    PaymentMethodRaw = SpreadsheetValueParser.CleanString(formaPagamento),
                    PaymentStatusRaw = SpreadsheetValueParser.CleanString(status),
                    DescriptionRaw = SpreadsheetValueParser.CleanString(descricao),
                    ObservationRaw = SpreadsheetValueParser.CleanString(obs),
                    SourceSheetName = ws.Name,
                    SourceFileType = "creche_mensal",
                    ReferenceYear = referenceYear,
                    ReferenceMonth = month,
                    RawPayloadJson = JsonSerializer.Serialize(payload)
                });
            }
        }

        return items;
    }

    private static bool IsRelevantMonthlyWorksheet(IXLWorksheet ws)
    {
        if (IgnoredSheets.Any(x => ws.Name.Equals(x, StringComparison.OrdinalIgnoreCase)))
            return false;

        return MonthNameResolver.IsMonthlySheet(ws.Name);
    }

    private static int? DetectHeaderRow(IXLWorksheet ws)
    {
        var lastColumn = ws.LastColumnUsed()?.ColumnNumber() ?? 0;
        if (lastColumn == 0)
            return null;

        var scanUntil = Math.Min(15, ws.LastRowUsed()?.RowNumber() ?? 15);

        for (var row = 1; row <= scanUntil; row++)
        {
            var score = 0;

            for (var col = 1; col <= lastColumn; col++)
            {
                var text = ws.Cell(row, col).GetString().Trim();
                if (MatchesAnyAlias(text, "STATUS")) score++;
                if (MatchesAnyAlias(text, "TIPO")) score++;
                if (MatchesAnyAlias(text, "CACHORRO")) score++;
                if (MatchesAnyAlias(text, "TUTOR")) score++;
                if (MatchesAnyAlias(text, "VALOR")) score++;
                if (MatchesAnyAlias(text, "FORMA DE PAGAMENTO")) score++;
            }

            if (score >= 3)
                return row;
        }

        return null;
    }

    private static Dictionary<string, int> BuildAliasMap(IXLWorksheet ws, int headerRow, int lastColumn)
    {
        var map = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

        for (var col = 1; col <= lastColumn; col++)
        {
            var text = ws.Cell(headerRow, col).GetString().Trim();

            foreach (var key in HeaderAliases.Keys)
            {
                if (MatchesAnyAlias(text, key) && !map.ContainsKey(key))
                    map[key] = col;
            }
        }

        return map;
    }

    private static bool MatchesAnyAlias(string? candidate, string canonicalKey)
    {
        if (!HeaderAliases.TryGetValue(canonicalKey, out var aliases))
            return false;

        var normalizedCandidate = TextNormalizer.NormalizeKey(candidate);
        return aliases.Any(alias => normalizedCandidate == TextNormalizer.NormalizeKey(alias));
    }

    private static object? GetValue(IXLWorksheet ws, int row, Dictionary<string, int> map, string canonicalKey)
    {
        if (!map.TryGetValue(canonicalKey, out var col))
            return null;

        return ws.Cell(row, col).Value.ToString();
    }

    private static bool IsRowEmpty(params object?[] values)
        => values.All(v => string.IsNullOrWhiteSpace(Convert.ToString(v)));

    private static int? TryGetReferenceYear(string filePath, XLWorkbook workbook)
    {
        var fileYear = SpreadsheetValueParser.ParseYear(Path.GetFileNameWithoutExtension(filePath));
        if (fileYear.HasValue)
            return fileYear.Value;

        foreach (var ws in workbook.Worksheets)
        {
            var fromSheet = SpreadsheetValueParser.ParseYear(ws.Name);
            if (fromSheet.HasValue)
                return fromSheet.Value;
        }

        return DateTime.UtcNow.Year;
    }
}