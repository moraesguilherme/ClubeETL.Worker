namespace ClubeETL.Worker.Models;

public sealed class UnifiedPaymentRawRow
{
    public int SourceRowNumber { get; set; }

    public string SourceFileName { get; set; } = string.Empty;
    public string SourceSheetName { get; set; } = string.Empty;
    public string SourceFileType { get; set; } = "unified_payments_sheet";

    public string ExternalRowKey { get; set; } = string.Empty;
    public string SourceContentHash { get; set; } = string.Empty;

    public string? ServiceTypeRaw { get; set; }
    public string? CustomerNameRaw { get; set; }
    public string? CustomerDocumentRaw { get; set; }
    public string? CustomerPhoneRaw { get; set; }

    public string? RawPetNames { get; set; }

    public string? PlanNameRaw { get; set; }
    public string? PackageNameRaw { get; set; }

    public DateTime? StartDate { get; set; }
    public DateTime? EndDate { get; set; }
    public DateTime? CompetenceDate { get; set; }
    public DateTime? OccurredAt { get; set; }

    public decimal? GrossAmount { get; set; }
    public decimal? TaxiAmount { get; set; }
    public decimal? GroupTotalAmount { get; set; }
    public decimal? NetAmount { get; set; }

    public string? ObservationRaw { get; set; }
    public string? PaymentStatusRaw { get; set; }
    public string? PaymentMethodRaw { get; set; }

    public int? ReferenceYear { get; set; }
    public int? ReferenceMonth { get; set; }

    public string RawPayloadJson { get; set; } = "{}";
}