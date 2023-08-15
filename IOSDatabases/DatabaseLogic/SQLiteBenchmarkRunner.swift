//
//  SQLiteBenchmarkRunner.swift
//  IOSDatabases
//
//  Created by Tom Ebergen on 8/10/23.
//

import Foundation
import SQLite

final class SQLiteBenchmarkRunner : BenchmarkProtocol {
    
    static let filename = "/Users/tomebergen/IOSDatabases/trips.sqlite3"
    static func GetEmptyConnection() throws -> Connection {
        // Create our database and connection as described above
        do {
            let database = try Connection(filename)

            return database
        }
        catch {
            throw BenchmarkError.dataBaseLoadError
        }
    }
    
    static func GetTaxiFileName() -> String {
        return "/Users/tomebergen/IOSDatabases/taxi-one-month-subset.csv"
    }
    
    static func deleteAllTrips(sqlite_connection: Connection) throws {
        do {
            let trips = Table("Trips")
            let drop = trips.drop(ifExists: true)
            try sqlite_connection.run(drop)
        } catch {
            throw BenchmarkError.databaseDeleteError(reason: "Duckdb: could not delete trips")
        }
    }
    
    static func GetNumtrips(sqlite_connection: Connection) throws -> Int {
        let trips = Table("Trips")
        do {
            let _ = try sqlite_connection.scalar(trips.exists)
            return try sqlite_connection.scalar(trips.count)
        } catch {
            return 0
        }
    }
    
    static func ImportBatchData() throws {
        do {
            
            let database = try SQLiteBenchmarkRunner.GetEmptyConnection()
            
            let trips = Table("Trips")
            if (try GetNumtrips(sqlite_connection: database) != 0) {
                throw BenchmarkError.databaseNotEmpty(reason: "SQLlite database was not empty. Please run batch delete first")
            }
            
            let vendor_name = Expression<String>("vendor_name")
            let passenger_count = Expression<Int>("passenger_count")
            let trip_distance = Expression<Double>("trip_distance")
            let pickup_longitude = Expression<Double>("pickup_longitude")
            let pickup_latitude = Expression<Double>("pickup_latitude")
            let rate_code = Expression<String>("rate_code")
            let store_and_fwd = Expression<String>("store_and_fwd")
            let dropoff_longitude = Expression<Double>("dropoff_longitude")
            let dropoff_latitude = Expression<Double>("dropoff_latitude")
            let payment_type = Expression<String>("payment_type")
            let fare_amount = Expression<Double>("fare_amount")
            let extra = Expression<Double>("extra")
            let mta_tax = Expression<Double>("mta_tax")
            let tip_amount = Expression<Double>("tip_amount")
            let tolls_amount = Expression<Double>("tolls_amount")
            let total_amount = Expression<Double>("total_amount")
            let improvement_surcharge = Expression<Double>("improvement_surcharge")
            let congestion_surcharge = Expression<Double>("congestion_surcharge")
            let pickup_location_id = Expression<Int>("pickup_location_id")
            let dropoff_location_id = Expression<Int>("dropoff_location_id")
            let year = Expression<Int>("year")
            let month = Expression<Int>("month")
            
            try database.run(trips.create { trip in
                trip.column(vendor_name)
                trip.column(passenger_count)
                trip.column(trip_distance)
                trip.column(pickup_longitude)
                trip.column(pickup_latitude)
                trip.column(rate_code)
                trip.column(store_and_fwd)
                trip.column(dropoff_longitude)
                trip.column(dropoff_latitude)
                trip.column(payment_type)
                trip.column(fare_amount)
                trip.column(extra)
                trip.column(mta_tax)
                trip.column(tip_amount)
                trip.column(tolls_amount)
                trip.column(total_amount)
                trip.column(improvement_surcharge)
                trip.column(congestion_surcharge)
                trip.column(pickup_location_id)
                trip.column(dropoff_location_id)
                trip.column(year)
                trip.column(month)
            })
            
            let all_data = CSVTripReader.readCSV(inputFile: GetTaxiFileName(), separator: ",")
            try database.transaction {
                for row in all_data {
                    let parsed_row = row.components(separatedBy: ",")
                    if (parsed_row.count < 24) {
                        // empty row probably
                        break
                    }
                    try database.run(
                        trips.insert(
                            vendor_name           <- String(parsed_row[0]),
                            // pickup_datetime       <- Time.init(Date(parsed_row[1])),
                            // dropoff_datetime      <- Time.init(Date(parsed_row[2])),
                            passenger_count       <- Int(parsed_row[3]) ?? 0,
                            trip_distance         <- Double(parsed_row[4]) ?? 0,
                            pickup_longitude      <- Double(parsed_row[5]) ?? 0,
                            pickup_latitude       <- Double(parsed_row[6]) ?? 0,
                            rate_code             <- String(parsed_row[7]),
                            store_and_fwd         <- String(parsed_row[8]),
                            dropoff_longitude     <- Double(parsed_row[9]) ?? 0,
                            dropoff_latitude      <- Double(parsed_row[10]) ?? 0,
                            payment_type          <- String(parsed_row[11]),
                            fare_amount           <- Double(parsed_row[12]) ?? 0,
                            extra                 <- Double(parsed_row[13]) ?? 0,
                            mta_tax               <- Double(parsed_row[14]) ?? 0,
                            tip_amount            <- Double(parsed_row[15]) ?? 0,
                            tolls_amount          <- Double(parsed_row[16]) ?? 0,
                            total_amount          <- Double(parsed_row[17]) ?? 0,
                            improvement_surcharge <- Double(parsed_row[18]) ?? 0,
                            congestion_surcharge  <- Double(parsed_row[19]) ?? 0,
                            pickup_location_id    <- Int(parsed_row[20]) ?? 0,
                            dropoff_location_id   <- Int(parsed_row[21]) ?? 0,
                            year                  <- Int(parsed_row[22]) ?? 0,
                            month                 <- Int(parsed_row[23]) ?? 0
                        )
                    )
                }
            }
            
            let record_count = try GetNumtrips(sqlite_connection: database)
            if (record_count != CSVTripReader.NUM_TRIPS) {
                throw BenchmarkError.dataBaseImportError(reason: "Did not import 50000 records")
            }
            print("imported \(record_count) records into SQLite database")
        }
    }
    
    static func ImportSingleData() throws {
        print("duck simple import")
    }
    
    func UpdateBatchData() throws {
        
    }
    
    func UpdateSingleData() throws {
        
    }
    
    static func DeleteBatchData() throws {
        let database = try SQLiteBenchmarkRunner.GetEmptyConnection()
        
        let num_trips = try GetNumtrips(sqlite_connection: database)
        try SQLiteBenchmarkRunner.deleteAllTrips(sqlite_connection: database)
        
        print("deleted \(num_trips)")
    }
    
    func DeleteSingleData() throws {
        
    }

    
}
