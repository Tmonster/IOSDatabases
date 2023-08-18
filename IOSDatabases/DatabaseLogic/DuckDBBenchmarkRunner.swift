//
//  DuckDBBenchmarkRunner.swift
//  IOSDatabases
//
//  Created by Tom Ebergen on 8/15/23.
//

import Foundation
import DuckDB
import TabularData

final class DuckDBBenchmarkRunner : BenchmarkProtocol {
    
    let database: Database
    let connection: Connection
    static let filename = "/Users/tomebergen/IOSDatabases/taxis.duckdb"
    static let taxi_schema = "columns={'vendor_name' :  'VARCHAR', 'pickup_datetime' :  'VARCHAR', 'dropoff_datetime' :  'VARCHAR', 'passenger_count' :  'Int', 'trip_distance' :  'Double', 'pickup_longitude' :  'Double', 'pickup_latitude' :  'Double', 'rate_code' :  'VARCHAR', 'store_and_fwd' :  'VARCHAR', 'dropoff_longitude' :  'Double', 'dropoff_latitude' :  'Double', 'payment_type' :  'VARCHAR', 'fare_amount' :  'Double', 'extra' :  'Double', 'mta_tax' :  'Double', 'tip_amount' :  'Double', 'tolls_amount' :  'Double', 'total_amount' :  'Double', 'improvement_surcharge' :  'Double', 'congestion_surcharge' :  'Double', 'pickup_location_id' :  'Int', 'dropoff_location_id' :  'Int', 'year' :  'Int', 'month' :  'Int'}"
    
    static let lineitem_schema = "columns={'l_orderkey': 'Int', 'l_partkey': 'Int', 'l_suppkey': 'Int', 'l_linenumber': 'Int', 'l_quantity': 'Double', 'l_extendedprice': 'Double', 'l_discount': 'Double', 'l_tax': 'Double', 'l_returnflag': 'VARCHAR', 'l_linestatus': 'VARCHAR', 'l_shipdate': 'Date', 'l_commitdate': 'Date', 'l_receiptdate': 'Date', 'l_shipinstruct': 'VARCHAR', 'l_shipmode': 'VARCHAR', 'l_comment': 'VARCHAR'}"
    
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

    static func deleteAllRecords(duckdb_connection: DuckDBBenchmarkRunner) throws {
        do {
            try duckdb_connection.connection.execute("delete from \(CSVTripReader.getTableName()) where 1=1");
        } catch {
            throw BenchmarkError.databaseDeleteError(reason: "Duckdb: could not delete all \(CSVTripReader.getTableName()) records")
        }
    }
    
    static func getNumRecords(duckdb_connection: DuckDBBenchmarkRunner) throws -> Int {
        do {
            let num_trips = try duckdb_connection.connection.query("select count(*) from \(CSVTripReader.getTableName());")
            let count_col = num_trips[0].cast(to: Int.self)
            let count_val : Int = count_col[0] ?? 0
            return count_val
        } catch {
            // assume table doesn't exist return 0
            return 0
        }
    }
    
    static func ImportBatchData() throws {
        let instance = try GetDuckDBConnection()
        let filename = CSVTripReader.getCSVFile()
        
        let num_stored_records: Int = try getNumRecords(duckdb_connection: instance)
        
        var schema : String
        switch CSVTripReader.benchmark {
        case .Taxi_benchmark:
            schema = taxi_schema
        case .tpch_benchmark:
            schema = lineitem_schema
        }
        
        if (num_stored_records != 0) {
            throw BenchmarkError.databaseNotEmpty(reason: "Duckdb database not empty before batch import. First run batch delete")
        }
        
        
        try instance.connection.execute("Create or Replace Table \(CSVTripReader.getTableName()) as (select * from read_csv_auto('\(filename)', \(schema)))")
        // verify amount
        let num_inserted_records = try getNumRecords(duckdb_connection: instance)
        if (num_inserted_records != CSVTripReader.getNumCSVRecords()) {
            print("error during DUCKDB importing. Inserted count of \(num_inserted_records) doesn't match CSV trip count")
        }
        print("imported \(num_inserted_records) records into DuckDB database")
    }
    
    static func RunAggregateQuery() throws {
        let instance = try GetDuckDBConnection()
        let _ = try instance.connection.query(CSVTripReader.getAggregateQuery())
        print("got it")
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
        let num_trips = try DuckDBBenchmarkRunner.getNumRecords(duckdb_connection: instance)
        try deleteAllRecords(duckdb_connection: instance)
        print("deleted \(num_trips) trips")

    }
    
    func DeleteSingleData() throws {
        
    }
    
//    static func RunAggregateQuery() throws {
//        let instance = try GetDuckDBConnection()
//        let result = try instance.connection.query("Select avg(tip_amount/total_amount) * 100 as avg_tip, month from trips group by month")
//        let avg_tip = result[0].cast(to: Double.self)
//        let month = result[1].cast(to: Int.self)
//
//        let dataframe_result =  DataFrame(columns: [
//            TabularData.Column(avg_tip)
//              .eraseToAnyColumn(),
//            TabularData.Column(month)
//              .eraseToAnyColumn(),
//          ])
//        print(dataframe_result)
//    }
    
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
