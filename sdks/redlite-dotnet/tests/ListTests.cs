using Xunit;

namespace Redlite.Tests;

public class ListTests
{
    [Fact]
    public void LPush_AddsToHead()
    {
        using var db = RedliteDb.OpenMemory();

        var len = db.LPush("list", "a", "b", "c");

        Assert.Equal(3, len);
        Assert.Equal(new[] { "c", "b", "a" }, db.LRange("list", 0, -1));
    }

    [Fact]
    public void RPush_AddsToTail()
    {
        using var db = RedliteDb.OpenMemory();

        var len = db.RPush("list", "a", "b", "c");

        Assert.Equal(3, len);
        Assert.Equal(new[] { "a", "b", "c" }, db.LRange("list", 0, -1));
    }

    [Fact]
    public void LPop_RemovesFromHead()
    {
        using var db = RedliteDb.OpenMemory();

        db.RPush("list", "a", "b", "c");
        var result = db.LPop("list");

        Assert.Equal("a", result);
        Assert.Equal(2, db.LLen("list"));
    }

    [Fact]
    public void RPop_RemovesFromTail()
    {
        using var db = RedliteDb.OpenMemory();

        db.RPush("list", "a", "b", "c");
        var result = db.RPop("list");

        Assert.Equal("c", result);
        Assert.Equal(2, db.LLen("list"));
    }

    [Fact]
    public void LPop_EmptyList_ReturnsNull()
    {
        using var db = RedliteDb.OpenMemory();

        var result = db.LPop("nonexistent");

        Assert.Null(result);
    }

    [Fact]
    public void LLen_ReturnsLength()
    {
        using var db = RedliteDb.OpenMemory();

        db.RPush("list", "a", "b", "c");
        var len = db.LLen("list");

        Assert.Equal(3, len);
    }

    [Fact]
    public void LRange_ReturnsSubset()
    {
        using var db = RedliteDb.OpenMemory();

        db.RPush("list", "a", "b", "c", "d", "e");
        var result = db.LRange("list", 1, 3);

        Assert.Equal(new[] { "b", "c", "d" }, result);
    }

    [Fact]
    public void LRange_NegativeIndices()
    {
        using var db = RedliteDb.OpenMemory();

        db.RPush("list", "a", "b", "c");
        var result = db.LRange("list", -2, -1);

        Assert.Equal(new[] { "b", "c" }, result);
    }

    [Fact]
    public void LIndex_ReturnsElement()
    {
        using var db = RedliteDb.OpenMemory();

        db.RPush("list", "a", "b", "c");
        var result = db.LIndex("list", 1);

        Assert.Equal("b", result);
    }

    [Fact]
    public void LIndex_OutOfRange_ReturnsNull()
    {
        using var db = RedliteDb.OpenMemory();

        db.RPush("list", "a", "b", "c");
        var result = db.LIndex("list", 10);

        Assert.Null(result);
    }

    [Fact]
    public void LPop_WithCount()
    {
        using var db = RedliteDb.OpenMemory();

        db.RPush("list", "a", "b", "c", "d", "e");
        var result = db.LPop("list", 3);

        Assert.Equal(new[] { "a", "b", "c" }, result);
        Assert.Equal(2, db.LLen("list"));
    }

    [Fact]
    public void RPop_WithCount()
    {
        using var db = RedliteDb.OpenMemory();

        db.RPush("list", "a", "b", "c", "d", "e");
        var result = db.RPop("list", 2);

        Assert.Equal(new[] { "e", "d" }, result);
        Assert.Equal(3, db.LLen("list"));
    }
}
