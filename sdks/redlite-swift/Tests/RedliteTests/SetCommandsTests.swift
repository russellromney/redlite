import XCTest
@testable import Redlite

final class SetCommandsTests: XCTestCase {

    var db: Database!

    override func setUp() {
        super.setUp()
        db = try! Database.openMemory()
    }

    override func tearDown() {
        db = nil
        super.tearDown()
    }

    // MARK: - SADD Tests

    func testSadd() throws {
        let added1 = try db.sadd("set", "a")
        XCTAssertEqual(added1, 1)

        let added2 = try db.sadd("set", "a")
        XCTAssertEqual(added2, 0)  // Already exists

        let added3 = try db.sadd("set", "b", "c")
        XCTAssertEqual(added3, 2)
    }

    func testSaddMultiple() throws {
        let added = try db.sadd("set", members: ["a", "b", "c"])
        XCTAssertEqual(added, 3)
    }

    // MARK: - SREM Tests

    func testSrem() throws {
        try db.sadd("set", "a", "b", "c")

        let removed = try db.srem("set", "a", "d")
        XCTAssertEqual(removed, 1)

        let members = try db.smembersSet("set")
        XCTAssertEqual(members, Set(["b", "c"]))
    }

    // MARK: - SMEMBERS Tests

    func testSmembers() throws {
        try db.sadd("set", "x", "y", "z")
        let members = try db.smembersSet("set")
        XCTAssertEqual(members, Set(["x", "y", "z"]))
    }

    func testSmembersEmpty() throws {
        let members = try db.smembers("empty")
        XCTAssertTrue(members.isEmpty)
    }

    // MARK: - SISMEMBER Tests

    func testSismember() throws {
        try db.sadd("set", "a", "b")

        XCTAssertTrue(try db.sismember("set", member: "a"))
        XCTAssertFalse(try db.sismember("set", member: "c"))
    }

    // MARK: - SCARD Tests

    func testScard() throws {
        XCTAssertEqual(try db.scard("empty"), 0)

        try db.sadd("set", "a", "b", "c")
        XCTAssertEqual(try db.scard("set"), 3)
    }
}
