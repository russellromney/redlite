namespace Redlite;

/// <summary>
/// Options for JSON.SET command.
/// </summary>
public class JsonSetOptions
{
    /// <summary>
    /// Only set if key does not exist (NX flag).
    /// </summary>
    public bool Nx { get; set; }

    /// <summary>
    /// Only set if key exists (XX flag).
    /// </summary>
    public bool Xx { get; set; }
}
