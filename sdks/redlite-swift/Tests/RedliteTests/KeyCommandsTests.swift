import XCTest
@testable import Redlite

final class KeyCommandsTests: XCTestCase {

    var db: Database!

    override func setUp() {
        super.setUp()
        db = try! Database.openMemory()
    }

    override func tearDown() {
        db = nil
        super.tearDown()
    }

    // MARK: - DEL/EXISTS Tests

    func testDel() throws {
        try db.set("k1", value: "v1")
        try db.set("k2", value: "v2")

        let deleted = try db.del("k1", "k2", "k3")
        XCTAssertEqual(deleted, 2)

        XCTAssertNil(try db.get("k1"))
        XCTAssertNil(try db.get("k2"))
    }

    func testExists() throws {
        try db.set("k1", value: "v1")
        try db.set("k2", value: "v2")

        let count = try db.exists("k1", "k2", "k3")
        XCTAssertEqual(count, 2)
    }

    func testExistsSingleKey() throws {
        try db.set("key", value: "value")
        let count = try db.exists("key")
        XCTAssertEqual(count, 1)

        let countMissing = try db.exists("nonexistent")
        XCTAssertEqual(countMissing, 0)
    }

    // MARK: - TYPE Tests

    func testTypeString() throws {
        try db.set("key", value: "value")
        let type = try db.type("key")
        XCTAssertEqual(type, "string")
    }

    func testTypeList() throws {
        try db.rpush("list", "a")
        let type = try db.type("list")
        XCTAssertEqual(type, "list")
    }

    func testTypeSet() throws {
        try db.sadd("set", "a")
        let type = try db.type("set")
        XCTAssertEqual(type, "set")
    }

    func testTypeHash() throws {
        try db.hset("hash", field: "f", value: "v")
        let type = try db.type("hash")
        XCTAssertEqual(type, "hash")
    }

    func testTypeZset() throws {
        try db.zadd("zset", score: 1.0, member: "a")
        let type = try db.type("zset")
        XCTAssertEqual(type, "zset")
    }

    func testTypeNonexistent() throws {
        let type = try db.type("nonexistent")
        XCTAssertNil(type)
    }

    // MARK: - TTL Tests

    func testTtl() throws {
        try db.set("key", value: "value", ttl: 60)
        let ttl = try db.ttl("key")
        XCTAssertGreaterThan(ttl, 55)
        XCTAssertLessThanOrEqual(ttl, 60)
    }

    func testTtlNoExpiration() throws {
        try db.set("key", value: "value")
        let ttl = try db.ttl("key")
        XCTAssertEqual(ttl, -1)
    }

    func testTtlNonexistent() throws {
        let ttl = try db.ttl("nonexistent")
        XCTAssertEqual(ttl, -2)
    }

    func testPttl() throws {
        try db.psetex("key", milliseconds: 60000, value: "value")
        let pttl = try db.pttl("key")
        XCTAssertGreaterThan(pttl, 55000)
        XCTAssertLessThanOrEqual(pttl, 60000)
    }

    // MARK: - EXPIRE Tests

    func testExpire() throws {
        try db.set("key", value: "value")
        let result = try db.expire("key", seconds: 60)
        XCTAssertTrue(result)

        let ttl = try db.ttl("key")
        XCTAssertGreaterThan(ttl, 55)
    }

    func testExpireNonexistent() throws {
        let result = try db.expire("nonexistent", seconds: 60)
        XCTAssertFalse(result)
    }

    func testPexpire() throws {
        try db.set("key", value: "value")
        let result = try db.pexpire("key", milliseconds: 60000)
        XCTAssertTrue(result)
    }

    func testPersist() throws {
        try db.set("key", value: "value", ttl: 60)
        let result = try db.persist("key")
        XCTAssertTrue(result)

        let ttl = try db.ttl("key")
        XCTAssertEqual(ttl, -1)
    }

    // MARK: - RENAME Tests

    func testRename() throws {
        try db.set("old", value: "value")
        try db.rename("old", to: "new")

        XCTAssertNil(try db.get("old"))
        XCTAssertEqual(try db.getString("new"), "value")
    }

    func testRenamenx() throws {
        try db.set("a", value: "1")
        try db.set("b", value: "2")

        let result1 = try db.renamenx("a", to: "c")
        XCTAssertTrue(result1)

        try db.set("d", value: "4")
        let result2 = try db.renamenx("d", to: "c")
        XCTAssertFalse(result2)
    }

    // MARK: - KEYS Tests

    func testKeys() throws {
        try db.set("user:1", value: "a")
        try db.set("user:2", value: "b")
        try db.set("other", value: "c")

        let userKeys = try db.keys("user:*")
        XCTAssertEqual(Set(userKeys), Set(["user:1", "user:2"]))

        let allKeys = try db.keys("*")
        XCTAssertEqual(allKeys.count, 3)
    }

    // MARK: - DBSIZE/FLUSHDB Tests

    func testDbsize() throws {
        XCTAssertEqual(try db.dbsize(), 0)

        try db.set("k1", value: "v1")
        try db.set("k2", value: "v2")
        XCTAssertEqual(try db.dbsize(), 2)
    }

    func testFlushdb() throws {
        try db.set("k1", value: "v1")
        try db.set("k2", value: "v2")

        try db.flushdb()
        XCTAssertEqual(try db.dbsize(), 0)
    }
}
