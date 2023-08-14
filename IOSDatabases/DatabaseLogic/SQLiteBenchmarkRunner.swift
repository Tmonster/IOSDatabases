//
//  DuckDBLogic.swift
//  IOSDatabases
//
//  Created by Tom Ebergen on 8/10/23.
//

import Foundation
import SQLite

final class SQLiteBenchmarkRunner : BenchmarkProtocol {
    
    static func GetEmptyConnection() throws -> SQLiteBenchmarkRunner {
        // Create our database and connection as described above
        do {
            let url = URL(filePath: "/Users/tomebergen/IOS-benchmarks/taxis.duckdb")
            let database = try Database(store: .file(at: url))
            let connection = try database.connect()
            
            // Create our pre-populated ExoplanetStore instance
            return DuckDBBenchmarkRunner(
                database: database,
                connection: connection
            )
        }
        catch {
            throw BenchmarkError.dataBaseLoadError
        }
    }
    
    static func GetTaxiFileName() -> String {
        return "/Users/tomebergen/IOS-benchmarks/taxi-one-month-subset.csv"
    }
    
    static func deleteAllTrips(duckdb_connection: DuckDBBenchmarkRunner) throws {
        do {
            try duckdb_connection.connection.execute("drop table trips");
        } catch {
            throw BenchmarkError.databaseDeleteError(reason: "Duckdb: could not delete trips")
        }
    }
    
    func checkTrips(duckdb_connection: DuckDBBenchmarkRunner) throws {
        let tables = try duckdb_connection.connection.query("show tables;")
        if (tables.rowCount > 0) {
            throw BenchmarkError.databaseNotEmpty(reason: "Duckdb trips table exists")
        }
    }
    
    static func ImportBatchData() throws {

    }
    
    static func ImportSingleData() throws {
        print("duck simple import")
    }
    
    func UpdateBatchData() throws {
        
    }
    
    func UpdateSingleData() throws {
        
    }
    
    func DeleteBatchData() throws {
        
    }
    
    func DeleteSingleData() throws {
        
    }
    
    // export the data to csv, read from it and return the line by line data
    // this is faster than returning a duckdb result, as unwrapping DuckDB results
    // takes much longer than reading csv (something to figure out)

    
    // Let's make the initializer we defined previously
    // private. This prevents anyone accidentally instantiating
    // the store without having pre-loaded our Exoplanet CSV
    // into the database
      private init(database: Database, connection: Connection) {
        self.database = database
        self.connection = connection
      }
    
}
//
//  SQLiteBenchmarkRunner.swift
//  IOSDatabases
//
//  Created by Tom Ebergen on 14/08/2023.
//

import Foundation
