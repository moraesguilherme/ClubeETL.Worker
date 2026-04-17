namespace ClubeETL.Worker.Models;

public sealed class ImportRowModel
{
    public int SourceRowNumber { get; set; }

    public string? CustomerNameRaw { get; set; }
    public string? CustomerDocumentRaw { get; set; }
    public string? CustomerPhoneRaw { get; set; }

    public string? PetNameRaw { get; set; }

    public string? ServiceTypeRaw { get; set; }
    public string? LodgingTypeRaw { get; set; }

    public string? PlanNameRaw { get; set; }
    public string? PackageNameRaw { get; set; }

    public decimal? GrossAmount { get; set; }
    public decimal? TaxiAmount { get; set; }
    public decimal? NetAmount { get; set; }

    public DateTime? StartDate { get; set; }
    public DateTime? EndDate { get; set; }
    public DateTime? OccurredAt { get; set; }
    public DateTime? CompetenceDate { get; set; }

    public string? PaymentMethodRaw { get; set; }
    public string? PaymentStatusRaw { get; set; }

    public string? DescriptionRaw { get; set; }
    public string? ObservationRaw { get; set; }

    public string SourceSheetName { get; set; } = string.Empty;
    public string SourceFileType { get; set; } = string.Empty;

    public int? ReferenceYear { get; set; }
    public int? ReferenceMonth { get; set; }

    public string RawPayloadJson { get; set; } = "{}";
}