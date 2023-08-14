//
//  DuckDBLogic.swift
//  IOSDatabases
//
//  Created by Tom Ebergen on 8/10/23.
//

import Foundation
import DuckDB


final class DuckDBManager {
    
    let database: Database
    let connection: Connection
    
    static func getEmpty() throws -> DuckDBManager {
        // Create our database and connection as described above
        let database = try Database(store: .inMemory)
        let connection = try database.connect()
        
        
        print("created empty database")
        // Create our pre-populated ExoplanetStore instance
        return DuckDBManager(
            database: database,
            connection: connection
        )
    }
    
    func ImportData() throws {
        try connection.execute("import database '/Users/tomebergen/benchmarks/tpch';")
    }
    
    // export the data to csv, read from it and return the line by line data
    // this is faster than returning a duckdb result, as unwrapping DuckDB results
    // takes much longer than reading csv (something to figure out)
    func GetData() throws -> [String] {
        return ["wow"]
    }
    
    func ImportDataSingle(data: [String]) {
        
    }
    
    func UpdateDataBatch(data: [String]) {
        
    }
    
    func UpdateDataSingle(data: [String]) {
        
    }
    
    func DeleteDataSingle(data: [String]) {
        
    }
    
    func DeleteDataBatch(data: [String]) {
        
    }
    
    func DeleteSingle(data: [String]) {
        
    }
    
    // Let's make the initializer we defined previously
    // private. This prevents anyone accidentally instantiating
    // the store without having pre-loaded our Exoplanet CSV
    // into the database
      private init(database: Database, connection: Connection) {
        self.database = database
        self.connection = connection
      }
    
}
