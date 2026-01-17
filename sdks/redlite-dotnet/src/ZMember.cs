namespace Redlite;

/// <summary>
/// Represents a member of a sorted set with its score.
/// </summary>
public readonly struct ZMember : IEquatable<ZMember>
{
    /// <summary>
    /// The score of the member.
    /// </summary>
    public double Score { get; }

    /// <summary>
    /// The member value.
    /// </summary>
    public string Member { get; }

    /// <summary>
    /// Create a new ZMember with the given score and member.
    /// </summary>
    public ZMember(double score, string member)
    {
        Score = score;
        Member = member ?? throw new ArgumentNullException(nameof(member));
    }

    /// <summary>
    /// Deconstruct into score and member.
    /// </summary>
    public void Deconstruct(out double score, out string member)
    {
        score = Score;
        member = Member;
    }

    public override string ToString() => $"{Member}: {Score}";

    public override int GetHashCode() => HashCode.Combine(Score, Member);

    public override bool Equals(object? obj) => obj is ZMember other && Equals(other);

    public bool Equals(ZMember other) =>
        Score.Equals(other.Score) && Member == other.Member;

    public static bool operator ==(ZMember left, ZMember right) => left.Equals(right);

    public static bool operator !=(ZMember left, ZMember right) => !left.Equals(right);
}
