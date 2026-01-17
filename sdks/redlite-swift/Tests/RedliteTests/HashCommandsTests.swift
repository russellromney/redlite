import XCTest
@testable import Redlite

final class HashCommandsTests: XCTestCase {

    var db: Database!

    override func setUp() {
        super.setUp()
        db = try! Database.openMemory()
    }

    override func tearDown() {
        db = nil
        super.tearDown()
    }

    // MARK: - HSET/HGET Tests

    func testHsetAndHget() throws {
        try db.hset("hash", field: "name", value: "Alice")
        let result = try db.hgetString("hash", field: "name")
        XCTAssertEqual(result, "Alice")
    }

    func testHgetNonexistent() throws {
        let result = try db.hget("hash", field: "missing")
        XCTAssertNil(result)
    }

    func testHsetMultiple() throws {
        let count = try db.hset("hash", fields: [
            ("f1", Data("v1".utf8)),
            ("f2", Data("v2".utf8))
        ])
        XCTAssertEqual(count, 2)

        XCTAssertEqual(try db.hgetString("hash", field: "f1"), "v1")
        XCTAssertEqual(try db.hgetString("hash", field: "f2"), "v2")
    }

    func testHsetDict() throws {
        try db.hset("hash", ["name": "Bob", "age": "30"])
        XCTAssertEqual(try db.hgetString("hash", field: "name"), "Bob")
        XCTAssertEqual(try db.hgetString("hash", field: "age"), "30")
    }

    // MARK: - HDEL Tests

    func testHdel() throws {
        try db.hset("hash", ["f1": "v1", "f2": "v2", "f3": "v3"])

        let deleted = try db.hdel("hash", "f1", "f2", "f4")
        XCTAssertEqual(deleted, 2)

        XCTAssertNil(try db.hget("hash", field: "f1"))
        XCTAssertNotNil(try db.hget("hash", field: "f3"))
    }

    // MARK: - HEXISTS Tests

    func testHexists() throws {
        try db.hset("hash", field: "field", value: "value")

        XCTAssertTrue(try db.hexists("hash", field: "field"))
        XCTAssertFalse(try db.hexists("hash", field: "missing"))
    }

    // MARK: - HLEN Tests

    func testHlen() throws {
        XCTAssertEqual(try db.hlen("empty"), 0)

        try db.hset("hash", ["f1": "v1", "f2": "v2"])
        XCTAssertEqual(try db.hlen("hash"), 2)
    }

    // MARK: - HKEYS/HVALS Tests

    func testHkeys() throws {
        try db.hset("hash", ["a": "1", "b": "2", "c": "3"])
        let keys = try db.hkeys("hash")
        XCTAssertEqual(Set(keys), Set(["a", "b", "c"]))
    }

    func testHvals() throws {
        try db.hset("hash", ["a": "1", "b": "2", "c": "3"])
        let vals = try db.hvalsStrings("hash")
        XCTAssertEqual(Set(vals), Set(["1", "2", "3"]))
    }

    // MARK: - HINCRBY Tests

    func testHincrby() throws {
        try db.hset("hash", field: "counter", value: "10")
        let result = try db.hincrby("hash", field: "counter", increment: 5)
        XCTAssertEqual(result, 15)
    }

    func testHincrbyNewField() throws {
        let result = try db.hincrby("hash", field: "new", increment: 3)
        XCTAssertEqual(result, 3)
    }

    // MARK: - HGETALL Tests

    func testHgetall() throws {
        try db.hset("hash", ["name": "Alice", "age": "30"])
        let all = try db.hgetallStrings("hash")
        XCTAssertEqual(all, ["name": "Alice", "age": "30"])
    }

    func testHgetallEmpty() throws {
        let all = try db.hgetall("empty")
        XCTAssertTrue(all.isEmpty)
    }

    // MARK: - HMGET Tests

    func testHmget() throws {
        try db.hset("hash", ["a": "1", "b": "2", "c": "3"])
        let results = try db.hmgetStrings("hash", fields: ["a", "b", "missing"])

        XCTAssertEqual(results.count, 3)
        XCTAssertEqual(results[0], "1")
        XCTAssertEqual(results[1], "2")
        XCTAssertNil(results[2])
    }
}
