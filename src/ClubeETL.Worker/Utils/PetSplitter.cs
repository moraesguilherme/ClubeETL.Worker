namespace ClubeETL.Worker.Utils;

public static class PetSplitter
{
    public static IReadOnlyList<string> Split(string? rawPetNames)
    {
        if (string.IsNullOrWhiteSpace(rawPetNames))
        {
            return Array.Empty<string>();
        }

        var items = rawPetNames
            .Split('/', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(x => x.Trim())
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .ToArray();

        return items;
    }
}