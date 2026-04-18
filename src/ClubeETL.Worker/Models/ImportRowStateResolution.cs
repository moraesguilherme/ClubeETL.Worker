namespace ClubeETL.Worker.Models;

public sealed class ImportRowStateResolution
{
    public string State { get; set; } = string.Empty;
    public long? ExistingImportRowId { get; set; }

    public bool IsNew =>
        string.Equals(State, "new", StringComparison.OrdinalIgnoreCase);

    public bool IsUnchanged =>
        string.Equals(State, "unchanged", StringComparison.OrdinalIgnoreCase);

    public bool IsChanged =>
        string.Equals(State, "changed", StringComparison.OrdinalIgnoreCase);
}