using Xunit;

namespace Redlite.Tests;

public class StringTests
{
    [Fact]
    public void GetSet_BasicRoundtrip()
    {
        using var db = RedliteDb.OpenMemory();

        db.Set("key", "value");
        var result = db.GetString("key");

        Assert.Equal("value", result);
    }

    [Fact]
    public void Get_NonExistentKey_ReturnsNull()
    {
        using var db = RedliteDb.OpenMemory();

        var result = db.GetString("nonexistent");

        Assert.Null(result);
    }

    [Fact]
    public void SetEx_WithExpiration()
    {
        using var db = RedliteDb.OpenMemory();

        db.SetEx("key", 60, "value");
        var ttl = db.Ttl("key");

        Assert.True(ttl >= 59 && ttl <= 60);
    }

    [Fact]
    public void Incr_OnNewKey()
    {
        using var db = RedliteDb.OpenMemory();

        var result = db.Incr("counter");

        Assert.Equal(1, result);
    }

    [Fact]
    public void Incr_OnExistingKey()
    {
        using var db = RedliteDb.OpenMemory();

        db.Set("counter", "10");
        var result = db.Incr("counter");

        Assert.Equal(11, result);
    }

    [Fact]
    public void IncrBy_WithIncrement()
    {
        using var db = RedliteDb.OpenMemory();

        db.Set("counter", "5");
        var result = db.IncrBy("counter", 3);

        Assert.Equal(8, result);
    }

    [Fact]
    public void Decr_OnExistingKey()
    {
        using var db = RedliteDb.OpenMemory();

        db.Set("counter", "10");
        var result = db.Decr("counter");

        Assert.Equal(9, result);
    }

    [Fact]
    public void Append_ToExistingKey()
    {
        using var db = RedliteDb.OpenMemory();

        db.Set("key", "Hello");
        var len = db.Append("key", " World");

        Assert.Equal(11, len);
        Assert.Equal("Hello World", db.GetString("key"));
    }

    [Fact]
    public void Append_ToNewKey()
    {
        using var db = RedliteDb.OpenMemory();

        var len = db.Append("newkey", "value");

        Assert.Equal(5, len);
        Assert.Equal("value", db.GetString("newkey"));
    }

    [Fact]
    public void StrLen_ReturnsCorrectLength()
    {
        using var db = RedliteDb.OpenMemory();

        db.Set("key", "Hello");
        var len = db.StrLen("key");

        Assert.Equal(5, len);
    }

    [Fact]
    public void GetRange_ReturnsSubstring()
    {
        using var db = RedliteDb.OpenMemory();

        db.Set("key", "Hello World");
        var result = db.GetRange("key", 0, 4);

        Assert.Equal("Hello", result);
    }

    [Fact]
    public void GetDel_ReturnsAndDeletes()
    {
        using var db = RedliteDb.OpenMemory();

        db.Set("key", "value");
        var result = db.GetDelString("key");

        Assert.Equal("value", result);
        Assert.Null(db.GetString("key"));
    }

    [Fact]
    public void MGet_MultipleKeys()
    {
        using var db = RedliteDb.OpenMemory();

        db.Set("k1", "v1");
        db.Set("k2", "v2");
        var result = db.MGet("k1", "k2", "k3");

        Assert.Equal(3, result.Length);
        Assert.Equal("v1", result[0]);
        Assert.Equal("v2", result[1]);
        Assert.Null(result[2]);
    }

    [Fact]
    public void MSet_MultipleKeys()
    {
        using var db = RedliteDb.OpenMemory();

        var success = db.MSet(("k1", "v1"), ("k2", "v2"));

        Assert.True(success);
        Assert.Equal("v1", db.GetString("k1"));
        Assert.Equal("v2", db.GetString("k2"));
    }

    [Fact]
    public void IncrByFloat_WithIncrement()
    {
        using var db = RedliteDb.OpenMemory();

        db.Set("float", "10.5");
        var result = db.IncrByFloat("float", 0.1);

        Assert.True(Math.Abs(result - 10.6) < 0.001);
    }
}
