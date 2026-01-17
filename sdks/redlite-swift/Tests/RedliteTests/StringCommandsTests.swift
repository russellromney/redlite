import XCTest
@testable import Redlite

final class StringCommandsTests: XCTestCase {

    var db: Database!

    override func setUp() {
        super.setUp()
        db = try! Database.openMemory()
    }

    override func tearDown() {
        db = nil
        super.tearDown()
    }

    // MARK: - GET/SET Tests

    func testSetAndGet() throws {
        try db.set("key", value: "value")
        let result = try db.getString("key")
        XCTAssertEqual(result, "value")
    }

    func testGetNonexistent() throws {
        let result = try db.get("nonexistent")
        XCTAssertNil(result)
    }

    func testSetWithData() throws {
        let data = Data([0x00, 0x01, 0x02, 0xFF])
        try db.set("binary", value: data)
        let result = try db.get("binary")
        XCTAssertEqual(result, data)
    }

    func testSetWithTTL() throws {
        try db.set("expiring", value: "temp", ttl: 3600)
        let result = try db.getString("expiring")
        XCTAssertEqual(result, "temp")

        let ttl = try db.ttl("expiring")
        XCTAssertGreaterThan(ttl, 3590)
        XCTAssertLessThanOrEqual(ttl, 3600)
    }

    func testSetex() throws {
        try db.setex("key", seconds: 60, value: "value")
        let ttl = try db.ttl("key")
        XCTAssertGreaterThan(ttl, 55)
        XCTAssertLessThanOrEqual(ttl, 60)
    }

    func testPsetex() throws {
        try db.psetex("key", milliseconds: 60000, value: "value")
        let pttl = try db.pttl("key")
        XCTAssertGreaterThan(pttl, 55000)
        XCTAssertLessThanOrEqual(pttl, 60000)
    }

    func testGetdel() throws {
        try db.set("key", value: "value")
        let result = try db.getdelString("key")
        XCTAssertEqual(result, "value")

        let check = try db.get("key")
        XCTAssertNil(check)
    }

    // MARK: - INCR/DECR Tests

    func testIncr() throws {
        let result1 = try db.incr("counter")
        XCTAssertEqual(result1, 1)

        let result2 = try db.incr("counter")
        XCTAssertEqual(result2, 2)
    }

    func testDecr() throws {
        try db.set("counter", value: "10")
        let result = try db.decr("counter")
        XCTAssertEqual(result, 9)
    }

    func testIncrby() throws {
        try db.set("counter", value: "10")
        let result = try db.incrby("counter", increment: 5)
        XCTAssertEqual(result, 15)
    }

    func testDecrby() throws {
        try db.set("counter", value: "10")
        let result = try db.decrby("counter", decrement: 3)
        XCTAssertEqual(result, 7)
    }

    func testIncrbyfloat() throws {
        try db.set("float", value: "10.5")
        let result = try db.incrbyfloat("float", increment: 0.1)
        XCTAssertEqual(result, 10.6, accuracy: 0.001)
    }

    // MARK: - String Manipulation Tests

    func testAppend() throws {
        try db.set("key", value: "Hello")
        let length = try db.append("key", value: " World")
        XCTAssertEqual(length, 11)

        let result = try db.getString("key")
        XCTAssertEqual(result, "Hello World")
    }

    func testStrlen() throws {
        try db.set("key", value: "Hello")
        let length = try db.strlen("key")
        XCTAssertEqual(length, 5)
    }

    func testStrlenNonexistent() throws {
        let length = try db.strlen("nonexistent")
        XCTAssertEqual(length, 0)
    }

    func testGetrange() throws {
        try db.set("key", value: "Hello World")
        let result = try db.getrangeString("key", start: 0, end: 4)
        XCTAssertEqual(result, "Hello")
    }

    func testGetrangeNegativeIndex() throws {
        try db.set("key", value: "Hello World")
        let result = try db.getrangeString("key", start: -5, end: -1)
        XCTAssertEqual(result, "World")
    }

    func testSetrange() throws {
        try db.set("key", value: "Hello World")
        let length = try db.setrange("key", offset: 6, value: "Redis")
        XCTAssertEqual(length, 11)

        let result = try db.getString("key")
        XCTAssertEqual(result, "Hello Redis")
    }

    // MARK: - Multi-Key Tests

    func testMsetAndMget() throws {
        try db.mset([
            ("k1", "v1"),
            ("k2", "v2"),
            ("k3", "v3")
        ])

        let results = try db.mgetStrings(["k1", "k2", "k3", "k4"])
        XCTAssertEqual(results.count, 4)
        XCTAssertEqual(results[0], "v1")
        XCTAssertEqual(results[1], "v2")
        XCTAssertEqual(results[2], "v3")
        XCTAssertNil(results[3])
    }

    func testMsetWithDict() throws {
        try db.mset(["a": "1", "b": "2"])
        XCTAssertEqual(try db.getString("a"), "1")
        XCTAssertEqual(try db.getString("b"), "2")
    }
}
