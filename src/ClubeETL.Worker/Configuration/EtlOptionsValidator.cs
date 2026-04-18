using Microsoft.Extensions.Options;

namespace ClubeETL.Worker.Configuration;

public sealed class EtlOptionsValidator : IValidateOptions<EtlOptions>
{
    public ValidateOptionsResult Validate(string? name, EtlOptions options)
    {
        if (string.IsNullOrWhiteSpace(options.InputFolder))
            return ValidateOptionsResult.Fail("Etl:InputFolder e obrigatorio.");

        if (string.IsNullOrWhiteSpace(options.ProcessedFolder))
            return ValidateOptionsResult.Fail("Etl:ProcessedFolder e obrigatorio.");

        if (string.IsNullOrWhiteSpace(options.ErrorFolder))
            return ValidateOptionsResult.Fail("Etl:ErrorFolder e obrigatorio.");

        if (string.IsNullOrWhiteSpace(options.SourceSystem))
            return ValidateOptionsResult.Fail("Etl:SourceSystem e obrigatorio.");

        var mode = (options.Mode ?? string.Empty).Trim();
        if (!mode.Equals("Manual", StringComparison.OrdinalIgnoreCase) &&
            !mode.Equals("Watch", StringComparison.OrdinalIgnoreCase))
        {
            return ValidateOptionsResult.Fail("Etl:Mode deve ser Manual ou Watch.");
        }

        if (options.PollingIntervalSeconds < 5)
            return ValidateOptionsResult.Fail("Etl:PollingIntervalSeconds deve ser maior ou igual a 5.");

        return ValidateOptionsResult.Success;
    }
}