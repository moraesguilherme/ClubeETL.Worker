namespace ClubeETL.Worker.Configuration;

public sealed class EtlOptions
{
    public const string SectionName = "Etl";

    public string Mode { get; set; } = "Manual";
    public int PollingIntervalSeconds { get; set; } = 30;

    public string InputFolder { get; set; } = "data/input";
    public string ProcessedFolder { get; set; } = "data/processed";
    public string ErrorFolder { get; set; } = "data/error";

    public bool IncludeCsv { get; set; } = false;
    public string SourceSystem { get; set; } = "matilha_planilhas";
}