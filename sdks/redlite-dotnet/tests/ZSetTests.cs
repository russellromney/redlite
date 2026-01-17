using Xunit;

namespace Redlite.Tests;

public class ZSetTests
{
    [Fact]
    public void ZAdd_AddsMembersWithScores()
    {
        using var db = RedliteDb.OpenMemory();

        var added = db.ZAdd("zset",
            new ZMember(1.0, "a"),
            new ZMember(2.0, "b"),
            new ZMember(3.0, "c"));

        Assert.Equal(3, added);
    }

    [Fact]
    public void ZAdd_SingleMember()
    {
        using var db = RedliteDb.OpenMemory();

        var added = db.ZAdd("zset", 1.5, "member");

        Assert.Equal(1, added);
        Assert.Equal(1.5, db.ZScore("zset", "member"));
    }

    [Fact]
    public void ZScore_ReturnsScore()
    {
        using var db = RedliteDb.OpenMemory();

        db.ZAdd("zset", 2.5, "member");
        var score = db.ZScore("zset", "member");

        Assert.Equal(2.5, score);
    }

    [Fact]
    public void ZScore_NonExistent_ReturnsNull()
    {
        using var db = RedliteDb.OpenMemory();

        var score = db.ZScore("zset", "nonexistent");

        Assert.Null(score);
    }

    [Fact]
    public void ZRem_RemovesMembers()
    {
        using var db = RedliteDb.OpenMemory();

        db.ZAdd("zset", new ZMember(1, "a"), new ZMember(2, "b"), new ZMember(3, "c"));
        var removed = db.ZRem("zset", "a", "d");

        Assert.Equal(1, removed);
        Assert.Equal(2, db.ZCard("zset"));
    }

    [Fact]
    public void ZCard_ReturnsMemberCount()
    {
        using var db = RedliteDb.OpenMemory();

        db.ZAdd("zset", new ZMember(1, "a"), new ZMember(2, "b"), new ZMember(3, "c"));
        var card = db.ZCard("zset");

        Assert.Equal(3, card);
    }

    [Fact]
    public void ZCount_CountsInRange()
    {
        using var db = RedliteDb.OpenMemory();

        db.ZAdd("zset", new ZMember(1, "a"), new ZMember(2, "b"), new ZMember(3, "c"));
        var count = db.ZCount("zset", 1.5, 3.0);

        Assert.Equal(2, count);
    }

    [Fact]
    public void ZIncrBy_IncrementsScore()
    {
        using var db = RedliteDb.OpenMemory();

        db.ZAdd("zset", 1.0, "member");
        var newScore = db.ZIncrBy("zset", 2.5, "member");

        Assert.Equal(3.5, newScore);
    }

    [Fact]
    public void ZRange_ReturnsByIndex()
    {
        using var db = RedliteDb.OpenMemory();

        db.ZAdd("zset", new ZMember(1, "a"), new ZMember(2, "b"), new ZMember(3, "c"));
        var result = db.ZRange("zset", 0, 1);

        Assert.Equal(new[] { "a", "b" }, result);
    }

    [Fact]
    public void ZRange_AllMembers()
    {
        using var db = RedliteDb.OpenMemory();

        db.ZAdd("zset", new ZMember(3, "c"), new ZMember(1, "a"), new ZMember(2, "b"));
        var result = db.ZRange("zset", 0, -1);

        Assert.Equal(new[] { "a", "b", "c" }, result);
    }

    [Fact]
    public void ZRangeWithScores_ReturnsMembersAndScores()
    {
        using var db = RedliteDb.OpenMemory();

        db.ZAdd("zset", new ZMember(1.0, "a"), new ZMember(2.0, "b"));
        var result = db.ZRangeWithScores("zset", 0, -1);

        Assert.Equal(2, result.Length);
        Assert.Equal("a", result[0].Member);
        Assert.Equal(1.0, result[0].Score);
        Assert.Equal("b", result[1].Member);
        Assert.Equal(2.0, result[1].Score);
    }

    [Fact]
    public void ZRevRange_ReturnsReversed()
    {
        using var db = RedliteDb.OpenMemory();

        db.ZAdd("zset", new ZMember(1, "a"), new ZMember(2, "b"), new ZMember(3, "c"));
        var result = db.ZRevRange("zset", 0, -1);

        Assert.Equal(new[] { "c", "b", "a" }, result);
    }
}
