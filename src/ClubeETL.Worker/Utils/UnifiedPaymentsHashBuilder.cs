using System.Security.Cryptography;
using System.Text;
using ClubeETL.Worker.Models;

namespace ClubeETL.Worker.Utils;

public static class UnifiedPaymentsHashBuilder
{
    public static string Build(UnifiedPaymentRawRow row)
    {
        var payload = string.Join("||", new[]
        {
            Normalize(row.ServiceTypeRaw),
            Normalize(row.CustomerNameRaw),
            Normalize(row.CustomerDocumentRaw),
            Normalize(row.CustomerPhoneRaw),
            Normalize(row.RawPetNames),
            Normalize(row.PlanNameRaw),
            Normalize(row.PackageNameRaw),
            NormalizeDate(row.StartDate),
            NormalizeDate(row.EndDate),
            NormalizeDate(row.CompetenceDate),
            NormalizeDate(row.OccurredAt),
            NormalizeDecimal(row.TaxiAmount),
            NormalizeDecimal(row.GrossAmount),
            NormalizeDecimal(row.GroupTotalAmount),
            Normalize(row.ObservationRaw),
            Normalize(row.PaymentStatusRaw),
            Normalize(row.PaymentMethodRaw)
        });

        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(payload));
        return Convert.ToHexString(bytes).ToLowerInvariant();
    }

    private static string Normalize(string? value) =>
        value?.Trim() ?? string.Empty;

    private static string NormalizeDate(DateTime? value) =>
        value?.ToString("yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture) ?? string.Empty;

    private static string NormalizeDecimal(decimal? value) =>
        value?.ToString("0.############################", System.Globalization.CultureInfo.InvariantCulture) ?? string.Empty;
}