using System.Globalization;

namespace ClubeETL.Worker.Utils;

public static class SpreadsheetValueParser
{
    public static string? CleanString(object? value)
    {
        if (value is null)
            return null;

        var text = Convert.ToString(value)?.Trim();
        return string.IsNullOrWhiteSpace(text) ? null : text;
    }

    public static decimal? ParseDecimal(object? value)
    {
        var text = CleanString(value);
        if (text is null)
            return null;

        text = text.Replace("R$", "", StringComparison.OrdinalIgnoreCase).Trim();

        if (decimal.TryParse(text, NumberStyles.Any, new CultureInfo("pt-BR"), out var ptBr))
            return ptBr;

        if (decimal.TryParse(text, NumberStyles.Any, CultureInfo.InvariantCulture, out var inv))
            return inv;

        return null;
    }

    public static DateTime? ParseDate(object? value)
    {
        if (value is DateTime dt)
            return dt;

        var text = CleanString(value);
        if (text is null)
            return null;

        var formats = new[]
        {
            "dd/MM/yyyy",
            "d/M/yyyy",
            "dd/MM/yy",
            "d/M/yy",
            "yyyy-MM-dd",
            "dd-MM-yyyy"
        };

        if (DateTime.TryParseExact(text, formats, new CultureInfo("pt-BR"), DateTimeStyles.None, out var exact))
            return exact;

        if (DateTime.TryParse(text, new CultureInfo("pt-BR"), DateTimeStyles.None, out var parsed))
            return parsed;

        return null;
    }

    public static int? ParseYear(string? text)
    {
        if (string.IsNullOrWhiteSpace(text))
            return null;

        var digits = new string(text.Where(char.IsDigit).ToArray());
        if (digits.Length >= 4 && int.TryParse(digits[^4..], out var year) && year is >= 2000 and <= 2100)
            return year;

        return null;
    }
}