import XCTest
@testable import Redlite

final class ListCommandsTests: XCTestCase {

    var db: Database!

    override func setUp() {
        super.setUp()
        db = try! Database.openMemory()
    }

    override func tearDown() {
        db = nil
        super.tearDown()
    }

    // MARK: - LPUSH/RPUSH Tests

    func testLpush() throws {
        let len1 = try db.lpush("list", "a")
        XCTAssertEqual(len1, 1)

        let len2 = try db.lpush("list", "b")
        XCTAssertEqual(len2, 2)

        let items = try db.lrangeStrings("list", start: 0, stop: -1)
        XCTAssertEqual(items, ["b", "a"])
    }

    func testLpushMultiple() throws {
        let len = try db.lpush("list", values: ["a", "b", "c"])
        XCTAssertEqual(len, 3)

        // LPUSH pushes in reverse order (last arg is first in list)
        let items = try db.lrangeStrings("list", start: 0, stop: -1)
        XCTAssertEqual(items, ["c", "b", "a"])
    }

    func testRpush() throws {
        try db.rpush("list", "a")
        try db.rpush("list", "b")

        let items = try db.lrangeStrings("list", start: 0, stop: -1)
        XCTAssertEqual(items, ["a", "b"])
    }

    func testRpushMultiple() throws {
        let len = try db.rpush("list", values: ["a", "b", "c"])
        XCTAssertEqual(len, 3)

        let items = try db.lrangeStrings("list", start: 0, stop: -1)
        XCTAssertEqual(items, ["a", "b", "c"])
    }

    // MARK: - LPOP/RPOP Tests

    func testLpop() throws {
        try db.rpush("list", values: ["a", "b", "c"])

        let popped = try db.lpopStrings("list", count: 1)
        XCTAssertEqual(popped, ["a"])

        let remaining = try db.lrangeStrings("list", start: 0, stop: -1)
        XCTAssertEqual(remaining, ["b", "c"])
    }

    func testLpopMultiple() throws {
        try db.rpush("list", values: ["a", "b", "c", "d"])

        let popped = try db.lpopStrings("list", count: 2)
        XCTAssertEqual(popped, ["a", "b"])
    }

    func testLpopOne() throws {
        try db.rpush("list", "value")
        let result = try db.lpopOne("list")
        XCTAssertEqual(String(data: result!, encoding: .utf8), "value")
    }

    func testRpop() throws {
        try db.rpush("list", values: ["a", "b", "c"])

        let popped = try db.rpopStrings("list", count: 1)
        XCTAssertEqual(popped, ["c"])
    }

    func testRpopMultiple() throws {
        try db.rpush("list", values: ["a", "b", "c", "d"])

        let popped = try db.rpopStrings("list", count: 2)
        XCTAssertEqual(popped, ["d", "c"])
    }

    // MARK: - LLEN Tests

    func testLlen() throws {
        XCTAssertEqual(try db.llen("empty"), 0)

        try db.rpush("list", values: ["a", "b", "c"])
        XCTAssertEqual(try db.llen("list"), 3)
    }

    // MARK: - LRANGE Tests

    func testLrange() throws {
        try db.rpush("list", values: ["a", "b", "c", "d", "e"])

        let range1 = try db.lrangeStrings("list", start: 0, stop: 2)
        XCTAssertEqual(range1, ["a", "b", "c"])

        let range2 = try db.lrangeStrings("list", start: -3, stop: -1)
        XCTAssertEqual(range2, ["c", "d", "e"])

        let all = try db.lrangeStrings("list", start: 0, stop: -1)
        XCTAssertEqual(all, ["a", "b", "c", "d", "e"])
    }

    // MARK: - LINDEX Tests

    func testLindex() throws {
        try db.rpush("list", values: ["a", "b", "c"])

        XCTAssertEqual(try db.lindexString("list", index: 0), "a")
        XCTAssertEqual(try db.lindexString("list", index: 1), "b")
        XCTAssertEqual(try db.lindexString("list", index: 2), "c")
        XCTAssertEqual(try db.lindexString("list", index: -1), "c")
    }

    func testLindexOutOfRange() throws {
        try db.rpush("list", "a")
        let result = try db.lindex("list", index: 10)
        XCTAssertNil(result)
    }
}
