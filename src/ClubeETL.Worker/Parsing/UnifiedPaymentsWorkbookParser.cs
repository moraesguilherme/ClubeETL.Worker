using System.Text.Json;
using ClubeETL.Worker.Models;
using ClubeETL.Worker.Utils;
using ClosedXML.Excel;

namespace ClubeETL.Worker.Parsing;

public sealed class UnifiedPaymentsWorkbookParser
{
    private static readonly string[] RequiredHeaders =
    {
        "SERVICO",
        "TUTOR",
        "CPF",
        "TELEFONE",
        "PET",
        "PLANO",
        "PACOTE",
        "DATA ENTRADA HOTEL",
        "DATA SAIDA HOTEL",
        "COMPETENCIA",
        "TAXI",
        "VALOR POR CAO",
        "VALOR TOTAL",
        "OBSERVACAO",
        "STATUS PAGAMENTO",
        "FORMA DE PAGAMENTO",
        "DATA PAGAMENTO"
    };

    public IReadOnlyList<UnifiedPaymentRawRow> Parse(string filePath)
    {
        using var workbook = new XLWorkbook(filePath);

        foreach (var worksheet in workbook.Worksheets)
        {
            if (!TryFindHeaderRow(worksheet, out var headerRowNumber, out var headerMap))
            {
                continue;
            }

            var fileName = Path.GetFileName(filePath);

            return ParseWorksheet(
                worksheet,
                fileName,
                headerRowNumber,
                headerMap);
        }

        throw new InvalidOperationException(
            "Nenhuma worksheet com o cabecalho da planilha unica de pagamentos foi encontrada.");
    }

    private static IReadOnlyList<UnifiedPaymentRawRow> ParseWorksheet(
        IXLWorksheet worksheet,
        string fileName,
        int headerRowNumber,
        IReadOnlyDictionary<string, int> headerMap)
    {
        var rows = new List<UnifiedPaymentRawRow>();
        var lastRow = worksheet.LastRowUsed()?.RowNumber() ?? headerRowNumber;

        for (var rowNumber = headerRowNumber + 1; rowNumber <= lastRow; rowNumber++)
        {
            var row = worksheet.Row(rowNumber);

            if (IsEmptyRow(row, headerMap))
            {
                continue;
            }

            var raw = new UnifiedPaymentRawRow
            {
                SourceRowNumber = rowNumber,
                SourceFileName = fileName,
                SourceSheetName = worksheet.Name,
                SourceFileType = "unified_payments_sheet",

                ServiceTypeRaw = ReadString(row, headerMap, "SERVICO"),
                CustomerNameRaw = ReadString(row, headerMap, "TUTOR"),
                CustomerDocumentRaw = ReadString(row, headerMap, "CPF"),
                CustomerPhoneRaw = ReadString(row, headerMap, "TELEFONE"),
                RawPetNames = ReadString(row, headerMap, "PET"),
                PlanNameRaw = ReadString(row, headerMap, "PLANO"),
                PackageNameRaw = ReadString(row, headerMap, "PACOTE"),
                StartDate = ReadDate(row, headerMap, "DATA ENTRADA HOTEL"),
                EndDate = ReadDate(row, headerMap, "DATA SAIDA HOTEL"),
                CompetenceDate = ReadCompetenceDate(row, headerMap, "COMPETENCIA"),
                TaxiAmount = ReadDecimal(row, headerMap, "TAXI"),
                GrossAmount = ReadDecimal(row, headerMap, "VALOR POR CAO"),
                GroupTotalAmount = ReadDecimal(row, headerMap, "VALOR TOTAL"),
                ObservationRaw = ReadString(row, headerMap, "OBSERVACAO"),
                PaymentStatusRaw = ReadString(row, headerMap, "STATUS PAGAMENTO"),
                PaymentMethodRaw = ReadString(row, headerMap, "FORMA DE PAGAMENTO"),
                OccurredAt = ReadDate(row, headerMap, "DATA PAGAMENTO")
            };

            raw.NetAmount = raw.GroupTotalAmount;
            raw.ReferenceYear = raw.CompetenceDate?.Year;
            raw.ReferenceMonth = raw.CompetenceDate?.Month;
            raw.ExternalRowKey = UnifiedPaymentsRowKeyBuilder.Build(
                raw.SourceFileName,
                raw.SourceSheetName,
                raw.SourceRowNumber);

            raw.RawPayloadJson = BuildRawPayloadJson(raw);
            raw.SourceContentHash = UnifiedPaymentsHashBuilder.Build(raw);

            rows.Add(raw);
        }

        return rows;
    }

    private static bool TryFindHeaderRow(
        IXLWorksheet worksheet,
        out int headerRowNumber,
        out IReadOnlyDictionary<string, int> headerMap)
    {
        var lastRow = worksheet.LastRowUsed()?.RowNumber() ?? 0;
        var lastColumn = worksheet.LastColumnUsed()?.ColumnNumber() ?? 0;

        for (var rowNumber = 1; rowNumber <= Math.Min(lastRow, 15); rowNumber++)
        {
            var currentMap = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

            for (var columnNumber = 1; columnNumber <= lastColumn; columnNumber++)
            {
                var rawHeader = worksheet.Cell(rowNumber, columnNumber).GetValue<string>();
                var normalized = TextNormalizer.NormalizeKey(rawHeader);

                if (!string.IsNullOrWhiteSpace(normalized) && !currentMap.ContainsKey(normalized))
                {
                    currentMap[normalized] = columnNumber;
                }
            }

            if (RequiredHeaders.All(currentMap.ContainsKey))
            {
                headerRowNumber = rowNumber;
                headerMap = currentMap;
                return true;
            }
        }

        headerRowNumber = 0;
        headerMap = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        return false;
    }

    private static bool IsEmptyRow(IXLRow row, IReadOnlyDictionary<string, int> headerMap)
    {
        foreach (var columnNumber in headerMap.Values.Distinct())
        {
            var text = row.Cell(columnNumber).GetValue<string>();
            if (!string.IsNullOrWhiteSpace(text))
            {
                return false;
            }
        }

        return true;
    }

    private static string? ReadString(
        IXLRow row,
        IReadOnlyDictionary<string, int> headerMap,
        string normalizedHeader)
    {
        return !headerMap.TryGetValue(normalizedHeader, out var columnNumber)
            ? null
            : SpreadsheetValueParser.CleanString(row.Cell(columnNumber).Value);
    }

    private static decimal? ReadDecimal(
        IXLRow row,
        IReadOnlyDictionary<string, int> headerMap,
        string normalizedHeader)
    {
        return !headerMap.TryGetValue(normalizedHeader, out var columnNumber)
            ? null
            : SpreadsheetValueParser.ParseDecimal(row.Cell(columnNumber).Value);
    }

    private static DateTime? ReadDate(
        IXLRow row,
        IReadOnlyDictionary<string, int> headerMap,
        string normalizedHeader)
    {
        return !headerMap.TryGetValue(normalizedHeader, out var columnNumber)
            ? null
            : SpreadsheetValueParser.ParseDate(row.Cell(columnNumber).Value);
    }

    private static DateTime? ReadCompetenceDate(
        IXLRow row,
        IReadOnlyDictionary<string, int> headerMap,
        string normalizedHeader)
    {
        return !headerMap.TryGetValue(normalizedHeader, out var columnNumber)
            ? null
            : SpreadsheetValueParser.ParseCompetenceDate(row.Cell(columnNumber).Value);
    }

    private static string BuildRawPayloadJson(UnifiedPaymentRawRow row)
    {
        var payload = new
        {
            servico = row.ServiceTypeRaw,
            tutor = row.CustomerNameRaw,
            cpf = row.CustomerDocumentRaw,
            telefone = row.CustomerPhoneRaw,
            pet = row.RawPetNames,
            plano = row.PlanNameRaw,
            pacote = row.PackageNameRaw,
            dataEntradaHotel = row.StartDate?.ToString("yyyy-MM-dd"),
            dataSaidaHotel = row.EndDate?.ToString("yyyy-MM-dd"),
            competencia = row.CompetenceDate?.ToString("yyyy-MM-dd"),
            taxi = row.TaxiAmount,
            valorPorCao = row.GrossAmount,
            valorTotal = row.GroupTotalAmount,
            observacao = row.ObservationRaw,
            statusPagamento = row.PaymentStatusRaw,
            formaPagamento = row.PaymentMethodRaw,
            dataPagamento = row.OccurredAt?.ToString("yyyy-MM-dd")
        };

        return JsonSerializer.Serialize(payload);
    }
}