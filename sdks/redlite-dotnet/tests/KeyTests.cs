using Xunit;

namespace Redlite.Tests;

public class KeyTests
{
    [Fact]
    public void Del_SingleKey()
    {
        using var db = RedliteDb.OpenMemory();

        db.Set("key", "value");
        var deleted = db.Del("key");

        Assert.Equal(1, deleted);
        Assert.Null(db.GetString("key"));
    }

    [Fact]
    public void Del_MultipleKeys()
    {
        using var db = RedliteDb.OpenMemory();

        db.Set("k1", "v1");
        db.Set("k2", "v2");
        var deleted = db.Del("k1", "k2", "k3");

        Assert.Equal(2, deleted);
    }

    [Fact]
    public void Exists_ReturnsTrueForExistingKey()
    {
        using var db = RedliteDb.OpenMemory();

        db.Set("key", "value");
        var exists = db.Exists("key");

        Assert.Equal(1, exists);
    }

    [Fact]
    public void Exists_ReturnsFalseForNonExistent()
    {
        using var db = RedliteDb.OpenMemory();

        var exists = db.Exists("nonexistent");

        Assert.Equal(0, exists);
    }

    [Fact]
    public void Type_ReturnsString()
    {
        using var db = RedliteDb.OpenMemory();

        db.Set("key", "value");
        var type = db.Type("key");

        Assert.Equal("string", type);
    }

    [Fact]
    public void Type_ReturnsNoneForNonExistent()
    {
        using var db = RedliteDb.OpenMemory();

        var type = db.Type("nonexistent");

        Assert.Equal("none", type);
    }

    [Fact]
    public void Ttl_ReturnsMinusTwoForNonExistent()
    {
        using var db = RedliteDb.OpenMemory();

        var ttl = db.Ttl("nonexistent");

        Assert.Equal(-2, ttl);
    }

    [Fact]
    public void Ttl_ReturnsMinusOneForNoExpiry()
    {
        using var db = RedliteDb.OpenMemory();

        db.Set("key", "value");
        var ttl = db.Ttl("key");

        Assert.Equal(-1, ttl);
    }

    [Fact]
    public void Expire_SetsExpiration()
    {
        using var db = RedliteDb.OpenMemory();

        db.Set("key", "value");
        var result = db.Expire("key", 60);
        var ttl = db.Ttl("key");

        Assert.True(result);
        Assert.True(ttl >= 59 && ttl <= 60);
    }

    [Fact]
    public void Persist_RemovesExpiration()
    {
        using var db = RedliteDb.OpenMemory();

        db.SetEx("key", 60, "value");
        var result = db.Persist("key");
        var ttl = db.Ttl("key");

        Assert.True(result);
        Assert.Equal(-1, ttl);
    }

    [Fact]
    public void Rename_RenamesKey()
    {
        using var db = RedliteDb.OpenMemory();

        db.Set("oldkey", "value");
        var result = db.Rename("oldkey", "newkey");

        Assert.True(result);
        Assert.Null(db.GetString("oldkey"));
        Assert.Equal("value", db.GetString("newkey"));
    }

    [Fact]
    public void RenameNx_OnlyIfNotExists()
    {
        using var db = RedliteDb.OpenMemory();

        db.Set("key1", "value1");
        db.Set("key2", "value2");
        var result = db.RenameNx("key1", "key2");

        Assert.False(result);
        Assert.Equal("value1", db.GetString("key1"));
    }

    [Fact]
    public void Keys_ReturnsMatchingKeys()
    {
        using var db = RedliteDb.OpenMemory();

        db.Set("user:1", "a");
        db.Set("user:2", "b");
        db.Set("post:1", "c");

        var keys = db.Keys("user:*");

        Assert.Equal(2, keys.Length);
        Assert.Contains("user:1", keys);
        Assert.Contains("user:2", keys);
    }

    [Fact]
    public void DbSize_ReturnsKeyCount()
    {
        using var db = RedliteDb.OpenMemory();

        db.Set("k1", "v1");
        db.Set("k2", "v2");
        var size = db.DbSize();

        Assert.Equal(2, size);
    }

    [Fact]
    public void FlushDb_ClearsDatabase()
    {
        using var db = RedliteDb.OpenMemory();

        db.Set("k1", "v1");
        db.Set("k2", "v2");
        db.FlushDb();
        var size = db.DbSize();

        Assert.Equal(0, size);
    }
}
