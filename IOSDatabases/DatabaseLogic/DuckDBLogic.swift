//
//  DuckDBLogic.swift
//  IOSDatabases
//
//  Created by Tom Ebergen on 8/10/23.
//

import Foundation
import DuckDB

final class DuckDBBenchmarkRunner : BenchmarkProtocol {
    
    let database: Database
    let connection: Connection
    static let filename = "taxis.duckdb"
    
    static func GetDuckDBConnection() throws -> DuckDBBenchmarkRunner {
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
        let instance = try GetDuckDBConnection()
        let filename = DuckDBBenchmarkRunner.GetTaxiFileName()
        
        let _ = checkTrips(instance)
        
        try instance.connection.execute("Create Table trips as (select * from read_csv_auto('\(filename)'))")
        // verify amount
        let result = try instance.connection.query("""
          Select * from trips;
        """)
        if (result.rowCount != CSVTripReader.NUM_TRIPS) {
            print("error during DUCKDB importing. Inserted count doesn't match CSV trip count")
        }
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
