using Xunit;

namespace Redlite.Tests;

public class SetTests
{
    [Fact]
    public void SAdd_AddsMembers()
    {
        using var db = RedliteDb.OpenMemory();

        var added = db.SAdd("set", "a", "b", "c");

        Assert.Equal(3, added);
    }

    [Fact]
    public void SAdd_IgnoresDuplicates()
    {
        using var db = RedliteDb.OpenMemory();

        db.SAdd("set", "a", "b");
        var added = db.SAdd("set", "b", "c");

        Assert.Equal(1, added);
        Assert.Equal(3, db.SCard("set"));
    }

    [Fact]
    public void SRem_RemovesMembers()
    {
        using var db = RedliteDb.OpenMemory();

        db.SAdd("set", "a", "b", "c");
        var removed = db.SRem("set", "a", "d");

        Assert.Equal(1, removed);
        Assert.Equal(2, db.SCard("set"));
    }

    [Fact]
    public void SMembers_ReturnsAllMembers()
    {
        using var db = RedliteDb.OpenMemory();

        db.SAdd("set", "a", "b", "c");
        var members = db.SMembers("set");

        Assert.Equal(3, members.Length);
        Assert.Contains("a", members);
        Assert.Contains("b", members);
        Assert.Contains("c", members);
    }

    [Fact]
    public void SIsMember_ReturnsTrueForMember()
    {
        using var db = RedliteDb.OpenMemory();

        db.SAdd("set", "a", "b", "c");

        Assert.True(db.SIsMember("set", "a"));
        Assert.False(db.SIsMember("set", "d"));
    }

    [Fact]
    public void SCard_ReturnsMemberCount()
    {
        using var db = RedliteDb.OpenMemory();

        db.SAdd("set", "a", "b", "c");
        var card = db.SCard("set");

        Assert.Equal(3, card);
    }

    [Fact]
    public void SCard_EmptySet_ReturnsZero()
    {
        using var db = RedliteDb.OpenMemory();

        var card = db.SCard("nonexistent");

        Assert.Equal(0, card);
    }
}
