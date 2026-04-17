namespace ClubeETL.Worker.Utils;

public static class MonthNameResolver
{
    private static readonly Dictionary<string, int> Months = new(StringComparer.OrdinalIgnoreCase)
    {
        ["JANEIRO"] = 1,
        ["FEVEREIRO"] = 2,
        ["MARCO"] = 3,
        ["MARÃ‡O"] = 3,
        ["ABRIL"] = 4,
        ["MAIO"] = 5,
        ["JUNHO"] = 6,
        ["JULHO"] = 7,
        ["AGOSTO"] = 8,
        ["SETEMBRO"] = 9,
        ["OUTUBRO"] = 10,
        ["NOVEMBRO"] = 11,
        ["DEZEMBRO"] = 12
    };

    public static bool TryGetMonth(string? sheetName, out int month)
    {
        month = 0;
        if (string.IsNullOrWhiteSpace(sheetName))
            return false;

        var normalized = TextNormalizer.NormalizeKey(sheetName);

        foreach (var pair in Months)
        {
            if (normalized.Contains(TextNormalizer.NormalizeKey(pair.Key), StringComparison.Ordinal))
            {
                month = pair.Value;
                return true;
            }
        }

        return false;
    }

    public static bool IsMonthlySheet(string? sheetName)
        => TryGetMonth(sheetName, out _);
}