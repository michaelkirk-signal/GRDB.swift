import XCTest
#if GRDBCIPHER
    import GRDBCipher
#elseif GRDBCUSTOMSQLITE
    import GRDBCustomSQLite
#else
    import GRDB
#endif

// A type that adopts DatabaseValueConvertible but does not adopt StatementColumnConvertible
private struct Fetched: DatabaseValueConvertible {
    let int: Int
    init(int: Int) {
        self.int = int
    }
    var databaseValue: DatabaseValue {
        return int.databaseValue
    }
    static func fromDatabaseValue(_ dbValue: DatabaseValue) -> Fetched? {
        guard let int = Int.fromDatabaseValue(dbValue) else {
            return nil
        }
        return Fetched(int: int)
    }
}

class DatabaseValueConvertibleFetchTests: GRDBTestCase {
    
    // MARK: - DatabaseValueConvertible.fetch
    
    func testFetchCursor() throws {
        let dbQueue = try makeDatabaseQueue()
        try dbQueue.inDatabase { db in
            func test(_ cursor: DatabaseValueCursor<Fetched>) throws {
                XCTAssertEqual(try cursor.next()!.int, 1)
                XCTAssertEqual(try cursor.next()!.int, 2)
                XCTAssertTrue(try cursor.next() == nil) // end
                XCTAssertTrue(try cursor.next() == nil) // past the end
            }
            do {
                let sql = "SELECT 1 UNION ALL SELECT 2"
                let statement = try db.makeSelectStatement(sql)
                try test(Fetched.fetchCursor(db, rawSQL: sql))
                try test(Fetched.fetchCursor(statement))
                try test(Fetched.fetchCursor(db, literal: SQLLiteral(sql: sql)))
                try test(Fetched.fetchCursor(db, SQLRequest<Void>(rawSQL: sql)))
                try test(SQLRequest<Fetched>(rawSQL: sql).fetchCursor(db))
            }
            do {
                let sql = "SELECT 0, 1 UNION ALL SELECT 0, 2"
                let statement = try db.makeSelectStatement(sql)
                let adapter = SuffixRowAdapter(fromIndex: 1)
                try test(Fetched.fetchCursor(db, rawSQL: sql, adapter: adapter))
                try test(Fetched.fetchCursor(statement, adapter: adapter))
                try test(Fetched.fetchCursor(db, literal: SQLLiteral(sql: sql), adapter: adapter))
                try test(Fetched.fetchCursor(db, SQLRequest<Void>(rawSQL: sql, adapter: adapter)))
                try test(SQLRequest<Fetched>(rawSQL: sql, adapter: adapter).fetchCursor(db))
            }
        }
    }
    
    #if swift(>=5.0)
    func testFetchCursorWithInterpolation() throws {
        let dbQueue = try makeDatabaseQueue()
        try dbQueue.inDatabase { db in
            let cursor = try Fetched.fetchCursor(db, literal: """
                SELECT \(42)
                """)
            let fetched = try cursor.next()!
            XCTAssertEqual(fetched.int, 42)
        }
    }
    #endif

    func testFetchCursorStepFailure() throws {
        let dbQueue = try makeDatabaseQueue()
        let customError = NSError(domain: "Custom", code: 0xDEAD)
        dbQueue.add(function: DatabaseFunction("throw", argumentCount: 0, pure: true) { _ in throw customError })
        try dbQueue.inDatabase { db in
            func test(_ cursor: DatabaseValueCursor<Fetched>, sql: String) throws {
                do {
                    _ = try cursor.next()
                    XCTFail()
                } catch let error as DatabaseError {
                    XCTAssertEqual(error.resultCode, .SQLITE_ERROR)
                    XCTAssertEqual(error.message, "\(customError)")
                    XCTAssertEqual(error.sql!, sql)
                    XCTAssertEqual(error.description, "SQLite error 1 with statement `\(sql)`: \(customError)")
                }
                do {
                    _ = try cursor.next()
                    XCTFail()
                } catch let error as DatabaseError {
                    XCTAssertEqual(error.resultCode, .SQLITE_MISUSE)
                    XCTAssertEqual(error.message, "\(customError)")
                    XCTAssertEqual(error.sql!, sql)
                    XCTAssertEqual(error.description, "SQLite error 21 with statement `\(sql)`: \(customError)")
                }
            }
            do {
                let sql = "SELECT throw()"
                try test(Fetched.fetchCursor(db, rawSQL: sql), sql: sql)
                try test(Fetched.fetchCursor(db.makeSelectStatement(sql)), sql: sql)
                try test(Fetched.fetchCursor(db, literal: SQLLiteral(sql: sql)), sql: sql)
                try test(Fetched.fetchCursor(db, SQLRequest<Void>(rawSQL: sql)), sql: sql)
                try test(SQLRequest<Fetched>(rawSQL: sql).fetchCursor(db), sql: sql)
            }
            do {
                let sql = "SELECT 0, throw()"
                let adapter = SuffixRowAdapter(fromIndex: 1)
                try test(Fetched.fetchCursor(db, rawSQL: sql, adapter: adapter), sql: sql)
                try test(Fetched.fetchCursor(db.makeSelectStatement(sql), adapter: adapter), sql: sql)
                try test(Fetched.fetchCursor(db, literal: SQLLiteral(sql: sql), adapter: adapter), sql: sql)
                try test(Fetched.fetchCursor(db, SQLRequest<Void>(rawSQL: sql, adapter: adapter)), sql: sql)
                try test(SQLRequest<Fetched>(rawSQL: sql, adapter: adapter).fetchCursor(db), sql: sql)
            }
        }
    }

    func testFetchCursorCompilationFailure() throws {
        let dbQueue = try makeDatabaseQueue()
        try dbQueue.inDatabase { db in
            func test(_ cursor: @autoclosure () throws -> DatabaseValueCursor<Fetched>, sql: String) throws {
                do {
                    _ = try cursor()
                    XCTFail()
                } catch let error as DatabaseError {
                    XCTAssertEqual(error.resultCode, .SQLITE_ERROR)
                    XCTAssertEqual(error.message, "no such table: nonExistingTable")
                    XCTAssertEqual(error.sql!, sql)
                    XCTAssertEqual(error.description, "SQLite error 1 with statement `\(sql)`: no such table: nonExistingTable")
                }
            }
            do {
                let sql = "SELECT * FROM nonExistingTable"
                try test(Fetched.fetchCursor(db, rawSQL: sql), sql: sql)
                try test(Fetched.fetchCursor(db.makeSelectStatement(sql)), sql: sql)
                try test(Fetched.fetchCursor(db, literal: SQLLiteral(sql: sql)), sql: sql)
                try test(Fetched.fetchCursor(db, SQLRequest<Void>(rawSQL: sql)), sql: sql)
                try test(SQLRequest<Fetched>(rawSQL: sql).fetchCursor(db), sql: sql)
            }
            do {
                let sql = "SELECT * FROM nonExistingTable"
                let adapter = SuffixRowAdapter(fromIndex: 1)
                try test(Fetched.fetchCursor(db, rawSQL: sql, adapter: adapter), sql: sql)
                try test(Fetched.fetchCursor(db.makeSelectStatement(sql), adapter: adapter), sql: sql)
                try test(Fetched.fetchCursor(db, literal: SQLLiteral(sql: sql), adapter: adapter), sql: sql)
                try test(Fetched.fetchCursor(db, SQLRequest<Void>(rawSQL: sql, adapter: adapter)), sql: sql)
                try test(SQLRequest<Fetched>(rawSQL: sql, adapter: adapter).fetchCursor(db), sql: sql)
            }
        }
    }

    func testFetchAll() throws {
        let dbQueue = try makeDatabaseQueue()
        try dbQueue.inDatabase { db in
            func test(_ array: [Fetched]) {
                XCTAssertEqual(array.map { $0.int }, [1,2])
            }
            do {
                let sql = "SELECT 1 UNION ALL SELECT 2"
                let statement = try db.makeSelectStatement(sql)
                try test(Fetched.fetchAll(db, rawSQL: sql))
                try test(Fetched.fetchAll(statement))
                try test(Fetched.fetchAll(db, literal: SQLLiteral(sql: sql)))
                try test(Fetched.fetchAll(db, SQLRequest<Void>(rawSQL: sql)))
                try test(SQLRequest<Fetched>(rawSQL: sql).fetchAll(db))
            }
            do {
                let sql = "SELECT 0, 1 UNION ALL SELECT 0, 2"
                let statement = try db.makeSelectStatement(sql)
                let adapter = SuffixRowAdapter(fromIndex: 1)
                try test(Fetched.fetchAll(db, rawSQL: sql, adapter: adapter))
                try test(Fetched.fetchAll(statement, adapter: adapter))
                try test(Fetched.fetchAll(db, literal: SQLLiteral(sql: sql), adapter: adapter))
                try test(Fetched.fetchAll(db, SQLRequest<Void>(rawSQL: sql, adapter: adapter)))
                try test(SQLRequest<Fetched>(rawSQL: sql, adapter: adapter).fetchAll(db))
            }
        }
    }
    
    #if swift(>=5.0)
    func testFetchAllWithInterpolation() throws {
        let dbQueue = try makeDatabaseQueue()
        try dbQueue.inDatabase { db in
            let array = try Fetched.fetchAll(db, literal: """
                SELECT \(42)
                """)
            XCTAssertEqual(array[0].int, 42)
        }
    }
    #endif
    
    func testFetchAllStepFailure() throws {
        let dbQueue = try makeDatabaseQueue()
        let customError = NSError(domain: "Custom", code: 0xDEAD)
        dbQueue.add(function: DatabaseFunction("throw", argumentCount: 0, pure: true) { _ in throw customError })
        try dbQueue.inDatabase { db in
            func test(_ array: @autoclosure () throws -> [Fetched], sql: String) throws {
                do {
                    _ = try array()
                    XCTFail()
                } catch let error as DatabaseError {
                    XCTAssertEqual(error.resultCode, .SQLITE_ERROR)
                    XCTAssertEqual(error.message, "\(customError)")
                    XCTAssertEqual(error.sql!, sql)
                    XCTAssertEqual(error.description, "SQLite error 1 with statement `\(sql)`: \(customError)")
                }
            }
            do {
                let sql = "SELECT throw()"
                try test(Fetched.fetchAll(db, rawSQL: sql), sql: sql)
                try test(Fetched.fetchAll(db.makeSelectStatement(sql)), sql: sql)
                try test(Fetched.fetchAll(db, literal: SQLLiteral(sql: sql)), sql: sql)
                try test(Fetched.fetchAll(db, SQLRequest<Void>(rawSQL: sql)), sql: sql)
                try test(SQLRequest<Fetched>(rawSQL: sql).fetchAll(db), sql: sql)
            }
            do {
                let sql = "SELECT 0, throw()"
                let adapter = SuffixRowAdapter(fromIndex: 1)
                try test(Fetched.fetchAll(db, rawSQL: sql, adapter: adapter), sql: sql)
                try test(Fetched.fetchAll(db.makeSelectStatement(sql), adapter: adapter), sql: sql)
                try test(Fetched.fetchAll(db, literal: SQLLiteral(sql: sql), adapter: adapter), sql: sql)
                try test(Fetched.fetchAll(db, SQLRequest<Void>(rawSQL: sql, adapter: adapter)), sql: sql)
                try test(SQLRequest<Fetched>(rawSQL: sql, adapter: adapter).fetchAll(db), sql: sql)
            }
        }
    }

    func testFetchAllCompilationFailure() throws {
        let dbQueue = try makeDatabaseQueue()
        try dbQueue.inDatabase { db in
            func test(_ array: @autoclosure () throws -> [Fetched], sql: String) throws {
                do {
                    _ = try array()
                    XCTFail()
                } catch let error as DatabaseError {
                    XCTAssertEqual(error.resultCode, .SQLITE_ERROR)
                    XCTAssertEqual(error.message, "no such table: nonExistingTable")
                    XCTAssertEqual(error.sql!, sql)
                    XCTAssertEqual(error.description, "SQLite error 1 with statement `\(sql)`: no such table: nonExistingTable")
                }
            }
            do {
                let sql = "SELECT * FROM nonExistingTable"
                try test(Fetched.fetchAll(db, rawSQL: sql), sql: sql)
                try test(Fetched.fetchAll(db.makeSelectStatement(sql)), sql: sql)
                try test(Fetched.fetchAll(db, literal: SQLLiteral(sql: sql)), sql: sql)
                try test(Fetched.fetchAll(db, SQLRequest<Void>(rawSQL: sql)), sql: sql)
                try test(SQLRequest<Fetched>(rawSQL: sql).fetchAll(db), sql: sql)
            }
            do {
                let sql = "SELECT * FROM nonExistingTable"
                let adapter = SuffixRowAdapter(fromIndex: 1)
                try test(Fetched.fetchAll(db, rawSQL: sql, adapter: adapter), sql: sql)
                try test(Fetched.fetchAll(db.makeSelectStatement(sql), adapter: adapter), sql: sql)
                try test(Fetched.fetchAll(db, literal: SQLLiteral(sql: sql), adapter: adapter), sql: sql)
                try test(Fetched.fetchAll(db, SQLRequest<Void>(rawSQL: sql, adapter: adapter)), sql: sql)
                try test(SQLRequest<Fetched>(rawSQL: sql, adapter: adapter).fetchAll(db), sql: sql)
            }
        }
    }

    func testFetchOne() throws {
        let dbQueue = try makeDatabaseQueue()
        try dbQueue.inDatabase { db in
            do {
                func test(_ nilBecauseMissingRow: Fetched?) {
                    XCTAssertTrue(nilBecauseMissingRow == nil)
                }
                do {
                    let sql = "SELECT 1 WHERE 0"
                    let statement = try db.makeSelectStatement(sql)
                    try test(Fetched.fetchOne(db, rawSQL: sql))
                    try test(Fetched.fetchOne(statement))
                    try test(Fetched.fetchOne(db, literal: SQLLiteral(sql: sql)))
                    try test(Fetched.fetchOne(db, SQLRequest<Void>(rawSQL: sql)))
                    try test(SQLRequest<Fetched>(rawSQL: sql).fetchOne(db))
                }
                do {
                    let sql = "SELECT 0, 1 WHERE 0"
                    let statement = try db.makeSelectStatement(sql)
                    let adapter = SuffixRowAdapter(fromIndex: 1)
                    try test(Fetched.fetchOne(db, rawSQL: sql, adapter: adapter))
                    try test(Fetched.fetchOne(statement, adapter: adapter))
                    try test(Fetched.fetchOne(db, literal: SQLLiteral(sql: sql), adapter: adapter))
                    try test(Fetched.fetchOne(db, SQLRequest<Void>(rawSQL: sql, adapter: adapter)))
                    try test(SQLRequest<Fetched>(rawSQL: sql, adapter: adapter).fetchOne(db))
                }
            }
            do {
                func test(_ nilBecauseNull: Fetched?) {
                    XCTAssertTrue(nilBecauseNull == nil)
                }
                do {
                    let sql = "SELECT NULL"
                    let statement = try db.makeSelectStatement(sql)
                    try test(Fetched.fetchOne(db, rawSQL: sql))
                    try test(Fetched.fetchOne(statement))
                    try test(Fetched.fetchOne(db, literal: SQLLiteral(sql: sql)))
                    try test(Fetched.fetchOne(db, SQLRequest<Void>(rawSQL: sql)))
                    try test(SQLRequest<Fetched>(rawSQL: sql).fetchOne(db))
                }
                do {
                    let sql = "SELECT 0, NULL"
                    let statement = try db.makeSelectStatement(sql)
                    let adapter = SuffixRowAdapter(fromIndex: 1)
                    try test(Fetched.fetchOne(db, rawSQL: sql, adapter: adapter))
                    try test(Fetched.fetchOne(statement, adapter: adapter))
                    try test(Fetched.fetchOne(db, literal: SQLLiteral(sql: sql), adapter: adapter))
                    try test(Fetched.fetchOne(db, SQLRequest<Void>(rawSQL: sql, adapter: adapter)))
                    try test(SQLRequest<Fetched>(rawSQL: sql, adapter: adapter).fetchOne(db))
                }
            }
            do {
                func test(_ value: Fetched?) {
                    XCTAssertEqual(value!.int, 1)
                }
                do {
                    let sql = "SELECT 1"
                    let statement = try db.makeSelectStatement(sql)
                    try test(Fetched.fetchOne(db, rawSQL: sql))
                    try test(Fetched.fetchOne(statement))
                    try test(Fetched.fetchOne(db, literal: SQLLiteral(sql: sql)))
                    try test(Fetched.fetchOne(db, SQLRequest<Void>(rawSQL: sql)))
                    try test(SQLRequest<Fetched>(rawSQL: sql).fetchOne(db))
                }
                do {
                    let sql = "SELECT 0, 1"
                    let statement = try db.makeSelectStatement(sql)
                    let adapter = SuffixRowAdapter(fromIndex: 1)
                    try test(Fetched.fetchOne(db, rawSQL: sql, adapter: adapter))
                    try test(Fetched.fetchOne(statement, adapter: adapter))
                    try test(Fetched.fetchOne(db, literal: SQLLiteral(sql: sql), adapter: adapter))
                    try test(Fetched.fetchOne(db, SQLRequest<Void>(rawSQL: sql, adapter: adapter)))
                    try test(SQLRequest<Fetched>(rawSQL: sql, adapter: adapter).fetchOne(db))
                }
            }
        }
    }
    
    #if swift(>=5.0)
    func testFetchOneWithInterpolation() throws {
        let dbQueue = try makeDatabaseQueue()
        try dbQueue.inDatabase { db in
            let fetched = try Fetched.fetchOne(db, literal: """
                SELECT \(42)
                """)
            XCTAssertEqual(fetched!.int, 42)
        }
    }
    #endif
    
    func testFetchOneStepFailure() throws {
        let dbQueue = try makeDatabaseQueue()
        let customError = NSError(domain: "Custom", code: 0xDEAD)
        dbQueue.add(function: DatabaseFunction("throw", argumentCount: 0, pure: true) { _ in throw customError })
        try dbQueue.inDatabase { db in
            func test(_ value: @autoclosure () throws -> Fetched?, sql: String) throws {
                do {
                    _ = try value()
                    XCTFail()
                } catch let error as DatabaseError {
                    XCTAssertEqual(error.resultCode, .SQLITE_ERROR)
                    XCTAssertEqual(error.message, "\(customError)")
                    XCTAssertEqual(error.sql!, sql)
                    XCTAssertEqual(error.description, "SQLite error 1 with statement `\(sql)`: \(customError)")
                }
            }
            do {
                let sql = "SELECT throw()"
                try test(Fetched.fetchOne(db, rawSQL: sql), sql: sql)
                try test(Fetched.fetchOne(db.makeSelectStatement(sql)), sql: sql)
                try test(Fetched.fetchOne(db, literal: SQLLiteral(sql: sql)), sql: sql)
                try test(Fetched.fetchOne(db, SQLRequest<Void>(rawSQL: sql)), sql: sql)
                try test(SQLRequest<Fetched>(rawSQL: sql).fetchOne(db), sql: sql)
            }
            do {
                let sql = "SELECT 0, throw()"
                let adapter = SuffixRowAdapter(fromIndex: 1)
                try test(Fetched.fetchOne(db, rawSQL: sql, adapter: adapter), sql: sql)
                try test(Fetched.fetchOne(db.makeSelectStatement(sql), adapter: adapter), sql: sql)
                try test(Fetched.fetchOne(db, literal: SQLLiteral(sql: sql), adapter: adapter), sql: sql)
                try test(Fetched.fetchOne(db, SQLRequest<Void>(rawSQL: sql, adapter: adapter)), sql: sql)
                try test(SQLRequest<Fetched>(rawSQL: sql, adapter: adapter).fetchOne(db), sql: sql)
            }
        }
    }

    func testFetchOneCompilationFailure() throws {
        let dbQueue = try makeDatabaseQueue()
        try dbQueue.inDatabase { db in
            func test(_ value: @autoclosure () throws -> Fetched?, sql: String) throws {
                do {
                    _ = try value()
                    XCTFail()
                } catch let error as DatabaseError {
                    XCTAssertEqual(error.resultCode, .SQLITE_ERROR)
                    XCTAssertEqual(error.message, "no such table: nonExistingTable")
                    XCTAssertEqual(error.sql!, sql)
                    XCTAssertEqual(error.description, "SQLite error 1 with statement `\(sql)`: no such table: nonExistingTable")
                }
            }
            do {
                let sql = "SELECT * FROM nonExistingTable"
                try test(Fetched.fetchOne(db, rawSQL: sql), sql: sql)
                try test(Fetched.fetchOne(db.makeSelectStatement(sql)), sql: sql)
                try test(Fetched.fetchOne(db, literal: SQLLiteral(sql: sql)), sql: sql)
                try test(Fetched.fetchOne(db, SQLRequest<Void>(rawSQL: sql)), sql: sql)
                try test(SQLRequest<Fetched>(rawSQL: sql).fetchOne(db), sql: sql)
            }
            do {
                let sql = "SELECT * FROM nonExistingTable"
                let adapter = SuffixRowAdapter(fromIndex: 1)
                try test(Fetched.fetchOne(db, rawSQL: sql, adapter: adapter), sql: sql)
                try test(Fetched.fetchOne(db.makeSelectStatement(sql), adapter: adapter), sql: sql)
                try test(Fetched.fetchOne(db, literal: SQLLiteral(sql: sql), adapter: adapter), sql: sql)
                try test(Fetched.fetchOne(db, SQLRequest<Void>(rawSQL: sql, adapter: adapter)), sql: sql)
                try test(SQLRequest<Fetched>(rawSQL: sql, adapter: adapter).fetchOne(db), sql: sql)
            }
        }
    }

    // MARK: - Optional<DatabaseValueConvertible>.fetch

    func testOptionalFetchCursor() throws {
        let dbQueue = try makeDatabaseQueue()
        try dbQueue.inDatabase { db in
            func test(_ cursor: NullableDatabaseValueCursor<Fetched>) throws {
                XCTAssertEqual(try cursor.next()!!.int, 1)
                XCTAssertTrue(try cursor.next()! == nil)
                XCTAssertTrue(try cursor.next() == nil) // end
                XCTAssertTrue(try cursor.next() == nil) // past the end
            }
            do {
                let sql = "SELECT 1 UNION ALL SELECT NULL"
                let statement = try db.makeSelectStatement(sql)
                try test(Optional<Fetched>.fetchCursor(db, rawSQL: sql))
                try test(Optional<Fetched>.fetchCursor(statement))
                try test(Optional<Fetched>.fetchCursor(db, literal: SQLLiteral(sql: sql)))
                try test(Optional<Fetched>.fetchCursor(db, SQLRequest<Void>(rawSQL: sql)))
                try test(SQLRequest<Fetched?>(rawSQL: sql).fetchCursor(db))
            }
            do {
                let sql = "SELECT 0, 1 UNION ALL SELECT 0, NULL"
                let statement = try db.makeSelectStatement(sql)
                let adapter = SuffixRowAdapter(fromIndex: 1)
                try test(Optional<Fetched>.fetchCursor(db, rawSQL: sql, adapter: adapter))
                try test(Optional<Fetched>.fetchCursor(statement, adapter: adapter))
                try test(Optional<Fetched>.fetchCursor(db, literal: SQLLiteral(sql: sql), adapter: adapter))
                try test(Optional<Fetched>.fetchCursor(db, SQLRequest<Void>(rawSQL: sql, adapter: adapter)))
                try test(SQLRequest<Fetched?>(rawSQL: sql, adapter: adapter).fetchCursor(db))
            }
        }
    }
    
    #if swift(>=5.0)
    func testOptionalFetchCursorWithInterpolation() throws {
        let dbQueue = try makeDatabaseQueue()
        try dbQueue.inDatabase { db in
            let cursor = try Optional<Fetched>.fetchCursor(db, literal: """
                SELECT \(42)
                """)
            let fetched = try cursor.next()!
            XCTAssertEqual(fetched!.int, 42)
        }
    }
    #endif
    
    func testOptionalFetchCursorStepFailure() throws {
        let dbQueue = try makeDatabaseQueue()
        let customError = NSError(domain: "Custom", code: 0xDEAD)
        dbQueue.add(function: DatabaseFunction("throw", argumentCount: 0, pure: true) { _ in throw customError })
        try dbQueue.inDatabase { db in
            func test(_ cursor: NullableDatabaseValueCursor<Fetched>, sql: String) throws {
                do {
                    _ = try cursor.next()
                    XCTFail()
                } catch let error as DatabaseError {
                    XCTAssertEqual(error.resultCode, .SQLITE_ERROR)
                    XCTAssertEqual(error.message, "\(customError)")
                    XCTAssertEqual(error.sql!, sql)
                    XCTAssertEqual(error.description, "SQLite error 1 with statement `\(sql)`: \(customError)")
                }
                do {
                    _ = try cursor.next()
                    XCTFail()
                } catch let error as DatabaseError {
                    XCTAssertEqual(error.resultCode, .SQLITE_MISUSE)
                    XCTAssertEqual(error.message, "\(customError)")
                    XCTAssertEqual(error.sql!, sql)
                    XCTAssertEqual(error.description, "SQLite error 21 with statement `\(sql)`: \(customError)")
                }
            }
            do {
                let sql = "SELECT throw()"
                try test(Optional<Fetched>.fetchCursor(db, rawSQL: sql), sql: sql)
                try test(Optional<Fetched>.fetchCursor(db.makeSelectStatement(sql)), sql: sql)
                try test(Optional<Fetched>.fetchCursor(db, literal: SQLLiteral(sql: sql)), sql: sql)
                try test(Optional<Fetched>.fetchCursor(db, SQLRequest<Void>(rawSQL: sql)), sql: sql)
                try test(SQLRequest<Fetched?>(rawSQL: sql).fetchCursor(db), sql: sql)
            }
            do {
                let sql = "SELECT 0, throw()"
                let adapter = SuffixRowAdapter(fromIndex: 1)
                try test(Optional<Fetched>.fetchCursor(db, rawSQL: sql, adapter: adapter), sql: sql)
                try test(Optional<Fetched>.fetchCursor(db.makeSelectStatement(sql), adapter: adapter), sql: sql)
                try test(Optional<Fetched>.fetchCursor(db, literal: SQLLiteral(sql: sql), adapter: adapter), sql: sql)
                try test(Optional<Fetched>.fetchCursor(db, SQLRequest<Void>(rawSQL: sql, adapter: adapter)), sql: sql)
                try test(SQLRequest<Fetched?>(rawSQL: sql, adapter: adapter).fetchCursor(db), sql: sql)
            }
        }
    }

    func testOptionalFetchCursorCompilationFailure() throws {
        let dbQueue = try makeDatabaseQueue()
        try dbQueue.inDatabase { db in
            func test(_ cursor: @autoclosure () throws -> NullableDatabaseValueCursor<Fetched>, sql: String) throws {
                do {
                    _ = try cursor()
                    XCTFail()
                } catch let error as DatabaseError {
                    XCTAssertEqual(error.resultCode, .SQLITE_ERROR)
                    XCTAssertEqual(error.message, "no such table: nonExistingTable")
                    XCTAssertEqual(error.sql!, sql)
                    XCTAssertEqual(error.description, "SQLite error 1 with statement `\(sql)`: no such table: nonExistingTable")
                }
            }
            do {
                let sql = "SELECT * FROM nonExistingTable"
                try test(Optional<Fetched>.fetchCursor(db, rawSQL: sql), sql: sql)
                try test(Optional<Fetched>.fetchCursor(db.makeSelectStatement(sql)), sql: sql)
                try test(Optional<Fetched>.fetchCursor(db, literal: SQLLiteral(sql: sql)), sql: sql)
                try test(Optional<Fetched>.fetchCursor(db, SQLRequest<Void>(rawSQL: sql)), sql: sql)
                try test(SQLRequest<Fetched?>(rawSQL: sql).fetchCursor(db), sql: sql)
            }
            do {
                let sql = "SELECT * FROM nonExistingTable"
                let adapter = SuffixRowAdapter(fromIndex: 1)
                try test(Optional<Fetched>.fetchCursor(db, rawSQL: sql, adapter: adapter), sql: sql)
                try test(Optional<Fetched>.fetchCursor(db.makeSelectStatement(sql), adapter: adapter), sql: sql)
                try test(Optional<Fetched>.fetchCursor(db, literal: SQLLiteral(sql: sql), adapter: adapter), sql: sql)
                try test(Optional<Fetched>.fetchCursor(db, SQLRequest<Void>(rawSQL: sql, adapter: adapter)), sql: sql)
                try test(SQLRequest<Fetched?>(rawSQL: sql, adapter: adapter).fetchCursor(db), sql: sql)
            }
        }
    }
    
    func testOptionalFetchAll() throws {
        let dbQueue = try makeDatabaseQueue()
        try dbQueue.inDatabase { db in
            func test(_ array: [Fetched?]) {
                XCTAssertEqual(array.count, 2)
                XCTAssertEqual(array[0]!.int, 1)
                XCTAssertTrue(array[1] == nil)
            }
            do {
                let sql = "SELECT 1 UNION ALL SELECT NULL"
                let statement = try db.makeSelectStatement(sql)
                try test(Optional<Fetched>.fetchAll(db, rawSQL: sql))
                try test(Optional<Fetched>.fetchAll(statement))
                try test(Optional<Fetched>.fetchAll(db, literal: SQLLiteral(sql: sql)))
                try test(Optional<Fetched>.fetchAll(db, SQLRequest<Void>(rawSQL: sql)))
                try test(SQLRequest<Fetched?>(rawSQL: sql).fetchAll(db))
            }
            do {
                let sql = "SELECT 0, 1 UNION ALL SELECT 0, NULL"
                let statement = try db.makeSelectStatement(sql)
                let adapter = SuffixRowAdapter(fromIndex: 1)
                try test(Optional<Fetched>.fetchAll(db, rawSQL: sql, adapter: adapter))
                try test(Optional<Fetched>.fetchAll(statement, adapter: adapter))
                try test(Optional<Fetched>.fetchAll(db, literal: SQLLiteral(sql: sql), adapter: adapter))
                try test(Optional<Fetched>.fetchAll(db, SQLRequest<Void>(rawSQL: sql, adapter: adapter)))
                try test(SQLRequest<Fetched?>(rawSQL: sql, adapter: adapter).fetchAll(db))
            }
        }
    }
    
    #if swift(>=5.0)
    func testOptionalFetchAllWithInterpolation() throws {
        let dbQueue = try makeDatabaseQueue()
        try dbQueue.inDatabase { db in
            let array = try Optional<Fetched>.fetchAll(db, literal: """
                SELECT \(42)
                """)
            XCTAssertEqual(array[0]!.int, 42)
        }
    }
    #endif
    
    func testOptionalFetchAllStepFailure() throws {
        let dbQueue = try makeDatabaseQueue()
        let customError = NSError(domain: "Custom", code: 0xDEAD)
        dbQueue.add(function: DatabaseFunction("throw", argumentCount: 0, pure: true) { _ in throw customError })
        try dbQueue.inDatabase { db in
            func test(_ array: @autoclosure () throws -> [Fetched?], sql: String) throws {
                do {
                    _ = try array()
                    XCTFail()
                } catch let error as DatabaseError {
                    XCTAssertEqual(error.resultCode, .SQLITE_ERROR)
                    XCTAssertEqual(error.message, "\(customError)")
                    XCTAssertEqual(error.sql!, sql)
                    XCTAssertEqual(error.description, "SQLite error 1 with statement `\(sql)`: \(customError)")
                }
            }
            do {
                let sql = "SELECT throw()"
                try test(Optional<Fetched>.fetchAll(db, rawSQL: sql), sql: sql)
                try test(Optional<Fetched>.fetchAll(db.makeSelectStatement(sql)), sql: sql)
                try test(Optional<Fetched>.fetchAll(db, literal: SQLLiteral(sql: sql)), sql: sql)
                try test(Optional<Fetched>.fetchAll(db, SQLRequest<Void>(rawSQL: sql)), sql: sql)
                try test(SQLRequest<Fetched?>(rawSQL: sql).fetchAll(db), sql: sql)
            }
            do {
                let sql = "SELECT 0, throw()"
                let adapter = SuffixRowAdapter(fromIndex: 1)
                try test(Optional<Fetched>.fetchAll(db, rawSQL: sql, adapter: adapter), sql: sql)
                try test(Optional<Fetched>.fetchAll(db.makeSelectStatement(sql), adapter: adapter), sql: sql)
                try test(Optional<Fetched>.fetchAll(db, literal: SQLLiteral(sql: sql), adapter: adapter), sql: sql)
                try test(Optional<Fetched>.fetchAll(db, SQLRequest<Void>(rawSQL: sql, adapter: adapter)), sql: sql)
                try test(SQLRequest<Fetched?>(rawSQL: sql, adapter: adapter).fetchAll(db), sql: sql)
            }
        }
    }

    func testOptionalFetchAllCompilationFailure() throws {
        let dbQueue = try makeDatabaseQueue()
        try dbQueue.inDatabase { db in
            func test(_ array: @autoclosure () throws -> [Fetched?], sql: String) throws {
                do {
                    _ = try array()
                    XCTFail()
                } catch let error as DatabaseError {
                    XCTAssertEqual(error.resultCode, .SQLITE_ERROR)
                    XCTAssertEqual(error.message, "no such table: nonExistingTable")
                    XCTAssertEqual(error.sql!, sql)
                    XCTAssertEqual(error.description, "SQLite error 1 with statement `\(sql)`: no such table: nonExistingTable")
                }
            }
            do {
                let sql = "SELECT * FROM nonExistingTable"
                try test(Optional<Fetched>.fetchAll(db, rawSQL: sql), sql: sql)
                try test(Optional<Fetched>.fetchAll(db.makeSelectStatement(sql)), sql: sql)
                try test(Optional<Fetched>.fetchAll(db, literal: SQLLiteral(sql: sql)), sql: sql)
                try test(Optional<Fetched>.fetchAll(db, SQLRequest<Void>(rawSQL: sql)), sql: sql)
                try test(SQLRequest<Fetched?>(rawSQL: sql).fetchAll(db), sql: sql)
            }
            do {
                let sql = "SELECT * FROM nonExistingTable"
                let adapter = SuffixRowAdapter(fromIndex: 1)
                try test(Optional<Fetched>.fetchAll(db, rawSQL: sql, adapter: adapter), sql: sql)
                try test(Optional<Fetched>.fetchAll(db.makeSelectStatement(sql), adapter: adapter), sql: sql)
                try test(Optional<Fetched>.fetchAll(db, literal: SQLLiteral(sql: sql), adapter: adapter), sql: sql)
                try test(Optional<Fetched>.fetchAll(db, SQLRequest<Void>(rawSQL: sql, adapter: adapter)), sql: sql)
                try test(SQLRequest<Fetched?>(rawSQL: sql, adapter: adapter).fetchAll(db), sql: sql)
            }
        }
    }
}
