namespace ClubeETL.Worker.Configuration;

public sealed class EtlOptions
{
    public const string SectionName = "Etl";

    public string Mode { get; set; } = "Manual";
    public int PollingIntervalSeconds { get; set; } = 30;
    public string InputFolder { get; set; } = @"C:\Users\Guilherme\Desktop\ClubeMatilha\ClubeETL\data\input";
    public string ProcessedFolder { get; set; } = @"C:\Users\Guilherme\Desktop\ClubeMatilha\ClubeETL\data\processed";
    public string ErrorFolder { get; set; } = @"C:\Users\Guilherme\Desktop\ClubeMatilha\ClubeETL\data\data\error";
    public bool IncludeCsv { get; set; } = false;
    public string SourceSystem { get; set; } = "matilha_planilhas";
}