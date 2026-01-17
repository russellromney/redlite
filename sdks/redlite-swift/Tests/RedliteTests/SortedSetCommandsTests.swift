import XCTest
@testable import Redlite

final class SortedSetCommandsTests: XCTestCase {

    var db: Database!

    override func setUp() {
        super.setUp()
        db = try! Database.openMemory()
    }

    override func tearDown() {
        db = nil
        super.tearDown()
    }

    // MARK: - ZADD Tests

    func testZadd() throws {
        let added = try db.zadd("zset", score: 1.0, member: "a")
        XCTAssertEqual(added, 1)

        // Update existing
        let updated = try db.zadd("zset", score: 2.0, member: "a")
        XCTAssertEqual(updated, 0)

        let score = try db.zscore("zset", member: "a")
        XCTAssertEqual(score, 2.0)
    }

    func testZaddMultiple() throws {
        let added = try db.zadd("zset", members: [
            ZMember(score: 1.0, member: "a"),
            ZMember(score: 2.0, member: "b"),
            ZMember(score: 3.0, member: "c")
        ])
        XCTAssertEqual(added, 3)
    }

    func testZaddVariadic() throws {
        let added = try db.zadd("zset", (1.0, "one"), (2.0, "two"))
        XCTAssertEqual(added, 2)
    }

    // MARK: - ZREM Tests

    func testZrem() throws {
        try db.zadd("zset", (1.0, "a"), (2.0, "b"), (3.0, "c"))

        let removed = try db.zrem("zset", "a", "d")
        XCTAssertEqual(removed, 1)

        let members = try db.zrangeStrings("zset", start: 0, stop: -1)
        XCTAssertEqual(members, ["b", "c"])
    }

    // MARK: - ZSCORE Tests

    func testZscore() throws {
        try db.zadd("zset", score: 3.14, member: "pi")

        let score = try db.zscore("zset", member: "pi")
        XCTAssertEqual(score!, 3.14, accuracy: 0.001)
    }

    func testZscoreNonexistent() throws {
        let score = try db.zscore("zset", member: "missing")
        XCTAssertNil(score)
    }

    // MARK: - ZCARD Tests

    func testZcard() throws {
        XCTAssertEqual(try db.zcard("empty"), 0)

        try db.zadd("zset", (1.0, "a"), (2.0, "b"), (3.0, "c"))
        XCTAssertEqual(try db.zcard("zset"), 3)
    }

    // MARK: - ZCOUNT Tests

    func testZcount() throws {
        try db.zadd("zset", (1.0, "a"), (2.0, "b"), (3.0, "c"), (4.0, "d"), (5.0, "e"))

        let count = try db.zcount("zset", min: 2.0, max: 4.0)
        XCTAssertEqual(count, 3)
    }

    // MARK: - ZINCRBY Tests

    func testZincrby() throws {
        try db.zadd("zset", score: 10.0, member: "item")

        let newScore = try db.zincrby("zset", member: "item", increment: 5.0)
        XCTAssertEqual(newScore, 15.0)
    }

    func testZincrbyNewMember() throws {
        let score = try db.zincrby("zset", member: "new", increment: 1.0)
        XCTAssertEqual(score, 1.0)
    }

    // MARK: - ZRANGE Tests

    func testZrange() throws {
        try db.zadd("zset", (1.0, "a"), (2.0, "b"), (3.0, "c"))

        let all = try db.zrangeStrings("zset", start: 0, stop: -1)
        XCTAssertEqual(all, ["a", "b", "c"])

        let partial = try db.zrangeStrings("zset", start: 0, stop: 1)
        XCTAssertEqual(partial, ["a", "b"])
    }

    func testZrangeWithScores() throws {
        try db.zadd("zset", (1.0, "a"), (2.0, "b"), (3.0, "c"))

        let members = try db.zrangeWithScores("zset", start: 0, stop: -1)
        XCTAssertEqual(members.count, 3)
        XCTAssertEqual(members[0].memberString, "a")
        XCTAssertEqual(members[0].score, 1.0)
        XCTAssertEqual(members[1].memberString, "b")
        XCTAssertEqual(members[1].score, 2.0)
    }

    // MARK: - ZREVRANGE Tests

    func testZrevrange() throws {
        try db.zadd("zset", (1.0, "a"), (2.0, "b"), (3.0, "c"))

        let all = try db.zrevrangeStrings("zset", start: 0, stop: -1)
        XCTAssertEqual(all, ["c", "b", "a"])
    }

    func testZrevrangeWithScores() throws {
        try db.zadd("zset", (1.0, "a"), (2.0, "b"), (3.0, "c"))

        let members = try db.zrevrangeWithScores("zset", start: 0, stop: 1)
        XCTAssertEqual(members.count, 2)
        XCTAssertEqual(members[0].memberString, "c")
        XCTAssertEqual(members[0].score, 3.0)
    }
}
