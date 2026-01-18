namespace Redlite;

/// <summary>
/// Information about a key returned by KEYINFO command.
/// </summary>
public readonly struct KeyInfo : IEquatable<KeyInfo>
{
    /// <summary>
    /// The type of the key ("string", "hash", "list", "set", "zset", "json").
    /// </summary>
    public string KeyType { get; }

    /// <summary>
    /// Time to live in seconds (-1 if no TTL, -2 if key doesn't exist).
    /// </summary>
    public long Ttl { get; }

    /// <summary>
    /// Unix timestamp in milliseconds when the key was created.
    /// </summary>
    public long CreatedAt { get; }

    /// <summary>
    /// Unix timestamp in milliseconds when the key was last updated.
    /// </summary>
    public long UpdatedAt { get; }

    /// <summary>
    /// Create a new KeyInfo with the given values.
    /// </summary>
    public KeyInfo(string keyType, long ttl, long createdAt, long updatedAt)
    {
        KeyType = keyType ?? "none";
        Ttl = ttl;
        CreatedAt = createdAt;
        UpdatedAt = updatedAt;
    }

    /// <summary>
    /// Deconstruct into type, ttl, createdAt, and updatedAt.
    /// </summary>
    public void Deconstruct(out string keyType, out long ttl, out long createdAt, out long updatedAt)
    {
        keyType = KeyType;
        ttl = Ttl;
        createdAt = CreatedAt;
        updatedAt = UpdatedAt;
    }

    public override string ToString() => $"{KeyType} (ttl={Ttl}, created={CreatedAt}, updated={UpdatedAt})";

    public override int GetHashCode() => HashCode.Combine(KeyType, Ttl, CreatedAt, UpdatedAt);

    public override bool Equals(object? obj) => obj is KeyInfo other && Equals(other);

    public bool Equals(KeyInfo other) =>
        KeyType == other.KeyType && Ttl == other.Ttl && CreatedAt == other.CreatedAt && UpdatedAt == other.UpdatedAt;

    public static bool operator ==(KeyInfo left, KeyInfo right) => left.Equals(right);

    public static bool operator !=(KeyInfo left, KeyInfo right) => !left.Equals(right);
}
