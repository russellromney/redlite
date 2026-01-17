namespace Redlite;

/// <summary>
/// Options for the SET command.
/// </summary>
public class SetOptions
{
    /// <summary>
    /// EX seconds -- Set the specified expire time, in seconds.
    /// </summary>
    public long? Ex { get; set; }

    /// <summary>
    /// PX milliseconds -- Set the specified expire time, in milliseconds.
    /// </summary>
    public long? Px { get; set; }

    /// <summary>
    /// NX -- Only set the key if it does not already exist.
    /// </summary>
    public bool Nx { get; set; }

    /// <summary>
    /// XX -- Only set the key if it already exists.
    /// </summary>
    public bool Xx { get; set; }

    /// <summary>
    /// Create SetOptions with expiration in seconds.
    /// </summary>
    public static SetOptions WithEx(long seconds) => new() { Ex = seconds };

    /// <summary>
    /// Create SetOptions with expiration in milliseconds.
    /// </summary>
    public static SetOptions WithPx(long milliseconds) => new() { Px = milliseconds };

    /// <summary>
    /// Create SetOptions with NX flag (only set if not exists).
    /// </summary>
    public static SetOptions OnlyIfNotExists() => new() { Nx = true };

    /// <summary>
    /// Create SetOptions with XX flag (only set if exists).
    /// </summary>
    public static SetOptions OnlyIfExists() => new() { Xx = true };
}
