using Xunit;

namespace Redlite.Tests;

public class HashTests
{
    [Fact]
    public void HSetHGet_BasicRoundtrip()
    {
        using var db = RedliteDb.OpenMemory();

        db.HSet("hash", "field", "value");
        var result = db.HGet("hash", "field");

        Assert.Equal("value", result);
    }

    [Fact]
    public void HGet_NonExistentField_ReturnsNull()
    {
        using var db = RedliteDb.OpenMemory();

        var result = db.HGet("hash", "nonexistent");

        Assert.Null(result);
    }

    [Fact]
    public void HSet_MultipleFields()
    {
        using var db = RedliteDb.OpenMemory();

        db.HSet("hash", new Dictionary<string, string>
        {
            { "f1", "v1" },
            { "f2", "v2" }
        });

        Assert.Equal("v1", db.HGet("hash", "f1"));
        Assert.Equal("v2", db.HGet("hash", "f2"));
    }

    [Fact]
    public void HDel_DeletesFields()
    {
        using var db = RedliteDb.OpenMemory();

        db.HSet("hash", "f1", "v1");
        db.HSet("hash", "f2", "v2");
        var deleted = db.HDel("hash", "f1", "f2", "f3");

        Assert.Equal(2, deleted);
        Assert.Null(db.HGet("hash", "f1"));
    }

    [Fact]
    public void HExists_ReturnsTrueForExistingField()
    {
        using var db = RedliteDb.OpenMemory();

        db.HSet("hash", "field", "value");
        var exists = db.HExists("hash", "field");

        Assert.True(exists);
    }

    [Fact]
    public void HLen_ReturnsFieldCount()
    {
        using var db = RedliteDb.OpenMemory();

        db.HSet("hash", new Dictionary<string, string>
        {
            { "f1", "v1" },
            { "f2", "v2" },
            { "f3", "v3" }
        });
        var len = db.HLen("hash");

        Assert.Equal(3, len);
    }

    [Fact]
    public void HKeys_ReturnsAllFields()
    {
        using var db = RedliteDb.OpenMemory();

        db.HSet("hash", new Dictionary<string, string>
        {
            { "f1", "v1" },
            { "f2", "v2" }
        });
        var keys = db.HKeys("hash");

        Assert.Equal(2, keys.Length);
        Assert.Contains("f1", keys);
        Assert.Contains("f2", keys);
    }

    [Fact]
    public void HVals_ReturnsAllValues()
    {
        using var db = RedliteDb.OpenMemory();

        db.HSet("hash", new Dictionary<string, string>
        {
            { "f1", "v1" },
            { "f2", "v2" }
        });
        var vals = db.HVals("hash");

        Assert.Equal(2, vals.Length);
        Assert.Contains("v1", vals);
        Assert.Contains("v2", vals);
    }

    [Fact]
    public void HIncrBy_IncrementsField()
    {
        using var db = RedliteDb.OpenMemory();

        db.HSet("hash", "count", "10");
        var result = db.HIncrBy("hash", "count", 5);

        Assert.Equal(15, result);
    }

    [Fact]
    public void HGetAll_ReturnsAllFieldsAndValues()
    {
        using var db = RedliteDb.OpenMemory();

        db.HSet("hash", new Dictionary<string, string>
        {
            { "f1", "v1" },
            { "f2", "v2" }
        });
        var all = db.HGetAll("hash");

        Assert.Equal(2, all.Count);
        Assert.Equal("v1", all["f1"]);
        Assert.Equal("v2", all["f2"]);
    }

    [Fact]
    public void HMGet_ReturnsMultipleFields()
    {
        using var db = RedliteDb.OpenMemory();

        db.HSet("hash", new Dictionary<string, string>
        {
            { "f1", "v1" },
            { "f2", "v2" }
        });
        var result = db.HMGet("hash", "f1", "f2", "f3");

        Assert.Equal(3, result.Length);
        Assert.Equal("v1", result[0]);
        Assert.Equal("v2", result[1]);
        Assert.Null(result[2]);
    }
}
