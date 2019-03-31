import Foundation
import Dispatch
#if SWIFT_PACKAGE
    import CSQLite
#elseif !GRDBCUSTOMSQLITE && !GRDBCIPHER
    import SQLite3
#endif

/// Configuration for a DatabaseQueue or DatabasePool.
public struct Configuration {
    
    // MARK: - Misc options
    
    /// If true, foreign key constraints are checked.
    ///
    /// Default: true
    public var foreignKeysEnabled: Bool = true
    
    /// If true, database modifications are disallowed.
    ///
    /// Default: false
    public var readonly: Bool = false
    
    /// The database label.
    ///
    /// You can query this label at runtime:
    ///
    ///     var configuration = Configuration()
    ///     configuration.label = "MyDatabase"
    ///     let dbQueue = try DatabaseQueue(path: ..., configuration: configuration)
    ///
    ///     try dbQueue.read { db in
    ///         print(db.configuration.label) // Prints "MyDatabase"
    ///     }
    ///
    /// The database label is also used to name the various dispatch queues
    /// created by GRDB, visible in debugging sessions and crash logs. However
    /// those dispatch queue labels are intended for debugging only. Their
    /// format may change between GRDB releases. Applications should not depend
    /// on the GRDB dispatch queue labels.
    ///
    /// If the database label is nil, the current GRDB implementation uses the
    /// following dispatch queue labels:
    ///
    /// - `GRDB.DatabaseQueue`: the (unique) dispatch queue of a DatabaseQueue
    /// - `GRDB.DatabasePool.writer`: the (unique) writer dispatch queue of
    ///   a DatabasePool
    /// - `GRDB.DatabasePool.reader.N`, where N is 1, 2, ...: one of the reader
    ///   dispatch queue(s) of a DatabasePool. N grows with the number of SQLite
    ///   connections: it may get bigger than the maximum number of concurrent
    ///   readers, as SQLite connections get closed and new ones are opened.
    /// - `GRDB.DatabasePool.snapshot.N`: the dispatch queue of a
    ///   DatabaseSnapshot. N grows with the number of snapshots.
    ///
    /// If the database label is not nil, for example "MyDatabase", the current
    /// GRDB implementation uses the following dispatch queue labels:
    ///
    /// - `MyDatabase`: the (unique) dispatch queue of a DatabaseQueue
    /// - `MyDatabase.writer`: the (unique) writer dispatch queue of
    ///   a DatabasePool
    /// - `MyDatabase.reader.N`, where N is 1, 2, ...: one of the reader
    ///   dispatch queue(s) of a DatabasePool. N grows with the number of SQLite
    ///   connections: it may get bigger than the maximum number of concurrent
    ///   readers, as SQLite connections get closed and new ones are opened.
    /// - `MyDatabase.snapshot.N`: the dispatch queue of a
    ///   DatabaseSnapshot. N grows with the number of snapshots.
    ///
    /// The default label is nil.
    public var label: String? = nil
    
    /// A function that is called on every statement executed by the database.
    ///
    /// Default: nil
    public var trace: TraceFunction?
    
    
    // MARK: - Encryption
    
    #if SQLITE_HAS_CODEC

    mutating func change(passphrase newPassphrase: String) {
        if var existingCipherConfiguration = cipherConfiguration {
            existingCipherConfiguration.passphrase = newPassphrase
            cipherConfiguration = existingCipherConfiguration
        } else {
            cipherConfiguration = CipherConfiguration(passphrase: newPassphrase)
        }
    }

    // To enable database encryption provide a CipherConfiguration
    //
    //     var config = Configuration()
    //     config.cipherConfiguration = CipherConfiguration(passphrase: "secret")
    public var cipherConfiguration: CipherConfiguration? = nil

    #endif
    
    
    // MARK: - Transactions
    
    /// The default kind of transaction.
    ///
    /// Default: deferred
    public var defaultTransactionKind: Database.TransactionKind = .deferred
    
    /// If false, it is a programmer error to leave a transaction opened at the
    /// end of a database access block.
    ///
    /// For example:
    ///
    ///     let dbQueue = DatabaseQueue()
    ///
    ///     // fatal error: A transaction has been left opened at the end of a database access
    ///     try dbQueue.inDatabase { db in
    ///         try db.beginTransaction()
    ///     }
    ///
    /// If true, one can leave opened transaction at the end of database access
    /// blocks:
    ///
    ///     var config = Configuration()
    ///     config.allowsUnsafeTransactions = true
    ///     let dbQueue = DatabaseQueue(configuration: config)
    ///
    ///     try dbQueue.inDatabase { db in
    ///         try db.beginTransaction()
    ///     }
    ///
    ///     try dbQueue.inDatabase { db in
    ///         try db.commit()
    ///     }
    ///
    /// This configuration flag has no effect on DatabasePool readers: those
    /// never allow leaving a transaction opened at the end of a read access.
    ///
    /// Default: false
    public var allowsUnsafeTransactions: Bool = false
    
    // MARK: - Concurrency
    
    /// The behavior in case of SQLITE_BUSY error. See https://www.sqlite.org/rescode.html#busy
    ///
    /// Default: immediateError
    public var busyMode: Database.BusyMode = .immediateError
    
    /// The maximum number of concurrent readers (applies to database
    /// pools only).
    ///
    /// Default: 5
    public var maximumReaderCount: Int = 5
    
    /// The quality of service class for the work performed by the database.
    ///
    /// The quality of service is ignored if you supply a target queue.
    ///
    /// Default: .default (.unspecified on macOS < 10.10)
    public var qos: DispatchQoS
    
    /// The target queue for the work performed by the database.
    ///
    /// Default: nil
    public var targetQueue: DispatchQueue? = nil
    
    // MARK: - Factory Configuration
    
    /// Creates a factory configuration
    public init() {
        if #available(OSX 10.10, *) {
            qos = .default
        } else {
            qos = .unspecified
        }
    }
    
    
    // MARK: - Not Public
    
    var threadingMode: Database.ThreadingMode = .`default`
    var SQLiteConnectionDidOpen: (() -> ())?
    var SQLiteConnectionWillClose: ((SQLiteConnection) -> ())?
    var SQLiteConnectionDidClose: (() -> ())?
    var SQLiteOpenFlags: Int32 {
        let readWriteFlags = readonly ? SQLITE_OPEN_READONLY : (SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE)
        return threadingMode.SQLiteOpenFlags | readWriteFlags
    }
    
    func makeDispatchQueue(defaultLabel: String, purpose: String? = nil) -> DispatchQueue {
        let label = (self.label ?? defaultLabel) + (purpose.map { "." + $0 } ?? "")
        if let targetQueue = targetQueue {
            return DispatchQueue(label: label, target: targetQueue)
        } else {
            return DispatchQueue(label: label, qos: qos)
        }
    }
}

/// A tracing function that takes an SQL string.
public typealias TraceFunction = (String) -> Void

#if SQLITE_HAS_CODEC

public struct CipherConfiguration {

    /// The passphrase for the encrypted database.
    public var passphrase: String

    public enum Parameters {
        /// Use the default cipher parameters for whichever version of SQLCipher is
        /// currently installed
        case defaultParameters

        /// Use cipher parameters corresponding to the defaults of the specified major
        /// version.
        ///
        /// - parameters:
        ///     - version: The cipher_compatibility for the encrypted database.
        ///
        ///     See https://www.zetetic.net/sqlcipher/sqlcipher-api/#cipher_compatibility
        ///
        ///     Setting to 1, 2, or 3 wil; cause SQLCipher to operate with default settings
        ///     consistent with that major version number for the current connection.
        ///
        /// Note: Available with SQLCipher 4.0.1 or later
        case compatibility(version: UInt)

        /// Use explicit non-default cipher parameters
        ///
        /// - parameters:
        ///     - cipherPageSize: The cipher_page_size setting for the encrypted database.
        ///     See https://www.zetetic.net/sqlcipher/sqlcipher-api/#cipher_page_size
        ///
        ///     If nil, the default for the installed version of SQLCipher will be used.
        ///
        ///     - kdfIteration: The kdf_iter setting for the encrypted database.
        ///     See https://www.zetetic.net/sqlcipher/sqlcipher-api/#kdf_iter
        ///
        ///     If nil, the default for the installed version of SQLCipher will be used.
        case custom(cipherPageSize: Int?, kdfIteration: Int?)
    }

    public var parameters: Parameters = .defaultParameters

    public struct UnencryptedHeaderConfiguration {

        /// The cipher_plaintext_header_size for the encrypted database.
        ///
        /// from https://www.zetetic.net/sqlcipher/sqlcipher-api/#cipher_plaintext_header_size
        ///    > the recommended offset is currently 32
        public var unencryptedLength: UInt = 32

        /// When using unencrypted headers, you must specify how to get the salt to SQLCipher
        /// using a SaltSource.
        ///
        /// By default, SQLCipher looks for the database salt in the first bytes of the on-disk
        /// database file. However, when using "unencrypted headers", the first portion of the
        /// database file must be well known SQLite text instead of the salt.
        public enum SaltSource {
            /// Pass in a block which provides the salt
            case block(() -> (Data))

            /// If your CipherConfiguration.passphrase is in "Raw Key Data with Explicit Salt"
            /// format, then use this SaltSource. SQLCipher will automatically extract the salt
            /// from that CipherConfiguration.passphrase.
            ///
            /// See here for details on how you would format such a passphrase:
            ///    https://www.zetetic.net/sqlcipher/sqlcipher-api/#key
            case rawKeyDataWithExplicitSalt
        }
        public var saltSource: SaltSource

        public init(saltSource: SaltSource) {
            self.saltSource = saltSource
        }
    }

    /// Set `unencryptedHeaderConfiguration` if you have a SQLCipher database in a shared app
    /// container to avoid 0x10deadcc crashes.
    ///
    /// As a rule, when an app or extension is suspended, if that process holds a lock on a file in
    /// a shared container, iOS will kill that process with the exception code: 0x10deadcc.
    ///
    /// Because SQLite's ubiquitous WAL mode requires file locking to facilitate concurrency, iOS
    /// provides a special exemption to this rule for SQLite files. However, because SQLCipher
    /// databases are encrypted, iOS cannot recognize them as database files. The exemption does
    /// not apply, and iOS kills the process.
    ///
    /// The work around is to leave the first bytes of the SQLCipher database unencrypted.
    /// This allows iOS to recognize that the locked file is a SQLite database and avoids the
    /// crash.
    ///
    /// The data in the first bytes of the database file are boilerplate like "SQLite Format 3\0",
    /// not sensitive user data.
    ///
    /// See more in `UnencryptedHeaderConfiguration` and at:
    /// https://www.zetetic.net/sqlcipher/sqlcipher-api/#cipher_plaintext_header_size
    public var unencryptedHeaderConfiguration: UnencryptedHeaderConfiguration?

    public init(passphrase: String) {
        self.passphrase = passphrase
    }
}

#endif
