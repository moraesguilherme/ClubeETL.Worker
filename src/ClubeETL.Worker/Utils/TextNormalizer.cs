using System.Globalization;
using System.Text;

namespace ClubeETL.Worker.Utils;

public static class TextNormalizer
{
    public static string NormalizeKey(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return string.Empty;

        var formD = value.Trim().ToUpperInvariant().Normalize(NormalizationForm.FormD);
        var sb = new StringBuilder();

        foreach (var ch in formD)
        {
            var category = CharUnicodeInfo.GetUnicodeCategory(ch);
            if (category == UnicodeCategory.NonSpacingMark)
                continue;

            if (char.IsLetterOrDigit(ch))
            {
                sb.Append(ch);
                continue;
            }

            if (char.IsWhiteSpace(ch) || ch == '_' || ch == '-' || ch == '/' || ch == '.')
                sb.Append(' ');
        }

        return string.Join(' ', sb.ToString().Split(' ', StringSplitOptions.RemoveEmptyEntries));
    }

    public static bool ContainsNormalized(string? source, string expected)
    {
        var normalizedSource = NormalizeKey(source);
        var normalizedExpected = NormalizeKey(expected);

        return normalizedSource.Contains(normalizedExpected, StringComparison.Ordinal);
    }
}