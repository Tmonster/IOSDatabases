//
//  DuckDBBenchmarkRunner.swift
//  IOSDatabases
//
//  Created by Tom Ebergen on 8/15/23.
//

import Foundation
import DuckDB

final class DuckDBBenchmarkRunner : BenchmarkProtocol {
    
    let database: Database
    let connection: Connection
    static let filename = "/Users/tomebergen/IOSDatabases/taxis.duckdb"
    
    static func GetDuckDBConnection() throws -> DuckDBBenchmarkRunner {
        // Create our database and connection as described above
        do {
            let url = URL(filePath: filename)
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

    static func deleteAllTrips(duckdb_connection: DuckDBBenchmarkRunner) throws {
        do {
            try duckdb_connection.connection.execute("delete from trips where 1=1");
        } catch {
            throw BenchmarkError.databaseDeleteError(reason: "Duckdb: could not delete trips")
        }
    }
    
    static func getNumTrips(duckdb_connection: DuckDBBenchmarkRunner) throws -> UInt64 {
        let tables = try duckdb_connection.connection.query("show tables;")
        if (tables.rowCount > 0) {
            let num_trips = try duckdb_connection.connection.query("select * from trips;")
            return num_trips.rowCount
        }
        return tables.rowCount
    }
    
    static func ImportBatchData() throws {
        let instance = try GetDuckDBConnection()
        let filename = CSVTripReader.CSV_FILE
        
        if (try getNumTrips(duckdb_connection: instance) != 0) {
            throw BenchmarkError.databaseNotEmpty(reason: "Duckdb database not empty before batch import. First run batch delete")
        }
        
        try instance.connection.execute("Create Table trips as (select * from read_csv_auto('\(filename)'))")
        // verify amount
        let result = try instance.connection.query("""
          Select * from trips;
        """)
        if (result.rowCount != CSVTripReader.NUM_TRIPS) {
            print("error during DUCKDB importing. Inserted count doesn't match CSV trip count")
        }
        print("imported \(result.rowCount) records into DuckDB database")
    }
    
    static func ImportSingleData() throws {
        print("duck simple import")
    }
    
    func UpdateBatchData() throws {
        
    }
    
    func UpdateSingleData() throws {
        
    }
    
    static func DeleteBatchData() throws {
        let instance = try GetDuckDBConnection()
        let num_trips = try DuckDBBenchmarkRunner.getNumTrips(duckdb_connection: instance)
        try deleteAllTrips(duckdb_connection: instance)
        print("deleted \(num_trips) trips")

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
