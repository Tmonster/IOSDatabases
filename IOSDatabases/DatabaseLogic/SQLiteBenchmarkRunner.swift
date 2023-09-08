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
    static func GetConnection() throws -> Connection {
        // Create our database and connection as described above
        do {
            let database = try Connection(filename)

            return database
        }
        catch {
            throw BenchmarkError.dataBaseLoadError
        }
    }

    
    static func deleteAllTrips(sqlite_connection: Connection) throws {
        do {
            let trips = Table(BenchmarkManager.getTableName())
            let drop = trips.drop(ifExists: true)
            try sqlite_connection.run(drop)
        } catch {
            throw BenchmarkError.databaseDeleteError(reason: "SQLite: could not delete" + BenchmarkManager.getTableName())
        }
    }
    
    static func GetNumSQLiteRecords(sqlite_connection: Connection) throws -> Int {
        let table = Table(BenchmarkManager.getTableName())
        do {
            let _ = try sqlite_connection.scalar(table.exists)
            return try sqlite_connection.scalar(table.count)
        } catch {
            return 0
        }
    }
    
    static func ImportLineItem() throws {
        
        let database = try SQLiteBenchmarkRunner.GetConnection()
        
        let lineitem = Table(BenchmarkManager.getTableName())
        let l_orderkey = Expression<Int>("l_orderkey")
        let l_partkey = Expression<Int>("l_partkey")
        let l_suppkey = Expression<Int>("l_suppkey")
        let l_linenumber = Expression<Int>("l_linenumber")
        let l_quantity = Expression<Double>("l_quantity")
        let l_extendedprice = Expression<Double>("l_extendedprice")
        let l_discount = Expression<Double>("l_discount")
        let l_tax = Expression<Double>("l_tax")
        let l_returnflag = Expression<String>("l_returnflag")
        let l_linestatus = Expression<String>("l_linestatus")
        let l_shipdate = Expression<Date>("l_shipdate")
        let l_commitdate = Expression<Date>("l_commitdate")
        let l_receiptdate = Expression<Date>("l_receiptdate")
        let l_shipinstruct = Expression<String>("l_shipinstruct")
        let l_shipmode = Expression<String>("l_shipmode")
        let l_comment = Expression<String>("l_comment")
        
        try database.run(lineitem.create { item in
            item.column(l_orderkey)
            item.column(l_partkey)
            item.column(l_suppkey)
            item.column(l_linenumber)
            item.column(l_quantity)
            item.column(l_extendedprice)
            item.column(l_discount)
            item.column(l_tax)
            item.column(l_returnflag)
            item.column(l_linestatus)
            item.column(l_shipdate)
            item.column(l_commitdate)
            item.column(l_receiptdate)
            item.column(l_shipinstruct)
            item.column(l_shipmode)
            item.column(l_comment)
        })
        
        Task {
            let all_data = await BenchmarkManager.readCSV()
            let dateFormatter = DateFormatter()
            
            try database.transaction {
                for row in all_data {
                    let parsed_row = row.components(separatedBy: ",")
                    if (parsed_row.count < 13) {
                        // empty row probably
                        break
                    }
                    try database.run(
                        lineitem.insert(
                            l_orderkey  <- Int(parsed_row[0]) ?? 0,
                            l_partkey  <- Int(parsed_row[1]) ?? 0,
                            l_suppkey  <- Int(parsed_row[2]) ?? 0,
                            l_linenumber  <- Int(parsed_row[3]) ?? 0,
                            l_quantity  <- Double(parsed_row[4]) ?? 0,
                            l_extendedprice  <- Double(parsed_row[5]) ?? 0,
                            l_discount  <- Double(parsed_row[6]) ?? 0,
                            l_tax  <- Double(parsed_row[7]) ?? 0,
                            l_returnflag  <- String(parsed_row[8]),
                            l_linestatus  <- String(parsed_row[9]),
                            l_shipdate  <- dateFormatter.date(from: parsed_row[10]) ?? Date(),
                            l_commitdate  <- dateFormatter.date(from: parsed_row[11]) ?? Date(),
                            l_receiptdate  <- dateFormatter.date(from: parsed_row[12]) ?? Date(),
                            l_shipinstruct  <- String(parsed_row[13]),
                            l_shipmode  <- String(parsed_row[14]),
                            l_comment  <- String(parsed_row[15])
                        )
                    )
                }
            }
            
            let record_count = try GetNumSQLiteRecords(sqlite_connection: database)
            if (record_count != BenchmarkManager.getNumCSVRecords()) {
                throw BenchmarkError.dataBaseImportError(reason: "imported \(record_count) items and not \(BenchmarkManager.getNumCSVRecords())")
            }
            print("imported \(record_count) items into SQLite database")
        }
    }
    
    
    static func ImportStreamingHistory() throws {
        do {

            let database = try SQLiteBenchmarkRunner.GetConnection()

            let streaming_history = Table(BenchmarkManager.getTableName())
            if (try GetNumSQLiteRecords(sqlite_connection: database) != 0) {
                throw BenchmarkError.databaseNotEmpty(reason: "SQLite database was not empty. Please run batch delete first")
            }

            let end_time = Expression<Date>("end_time")
            let artist_name = Expression<String>("artist_name")
            let track_name = Expression<String>("track_name")
            let ms_played = Expression<Int>("ms_played")

            try database.run(streaming_history.create { stream in
                stream.column(end_time)
                stream.column(artist_name)
                stream.column(track_name)
                stream.column(ms_played)
            })
            
            let formatter = DateFormatter()
            formatter.dateFormat = "yyyy-MM-dd HH:mm"
            
            Task {
                let all_data = await BenchmarkManager.readCSV()
                try database.transaction {
                    for row in all_data {
                        let parsed_row = row.components(separatedBy: ",")
                        if (parsed_row.count < 4) {
                            // empty row probably
                            break
                        }
                        if let date_object = formatter.date(from: parsed_row[0]) {
                            try database.run(
                                streaming_history.insert(
                                    end_time          <- date_object,
                                    artist_name       <- String(parsed_row[1]),
                                    track_name        <- String(parsed_row[2]),
                                    ms_played         <- Int(parsed_row[3]) ?? 0
                                )
                            )
                        }
                    }
                }
                
                let record_count = try GetNumSQLiteRecords(sqlite_connection: database)
                if (record_count != BenchmarkManager.getNumCSVRecords()) {
                    throw BenchmarkError.dataBaseImportError(reason: "imported \(record_count) streams and not \(BenchmarkManager.getNumCSVRecords())")
                }
                print("imported \(record_count) records into SQLite database")
            }
        }
    }
    
    static func ImportTaxis() throws {
        do {

           let database = try SQLiteBenchmarkRunner.GetConnection()

           let trips = Table("Trips")
           if (try GetNumSQLiteRecords(sqlite_connection: database) != 0) {
               throw BenchmarkError.databaseNotEmpty(reason: "SQLite database was not empty. Please run batch delete first")
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

            Task {
                let all_data = await BenchmarkManager.readCSV()
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
                
                let record_count = try GetNumSQLiteRecords(sqlite_connection: database)
                if (record_count != BenchmarkManager.getNumCSVRecords()) {
                    throw BenchmarkError.dataBaseImportError(reason: "imported \(record_count) trips and not \(BenchmarkManager.getNumCSVRecords())")
                }
                print("imported \(record_count) records into SQLite database")
            }
       }
    }
    
    static func ImportBatchData() throws {
        switch BenchmarkManager.benchmark {
        case .Taxi_benchmark:
            try ImportTaxis()
        case .tpch_benchmark:
            try ImportLineItem()
        case .spotify_benchmark:
            try ImportStreamingHistory()
        }
    }
    
    func UpdateBatchData() throws {
        
    }
    
    func UpdateSingleData() throws {
        
    }
    
    
    static func RunAggregateQuery(num : Int) throws {
        let database = try SQLiteBenchmarkRunner.GetConnection()
        var query : String = ""
        if (num == 1) {
            query = BenchmarkManager.getAggregateQuery1()
        } else if (num == 2) {
            query = BenchmarkManager.getAggregateQuery2()
        } else if (num == 3) {
            query = BenchmarkManager.getAggregateQuery3()
        }
        do {
            for row in try database.prepare(query) {
                print("ret flag = \(row[0])")
            }
        }
    }
    
    static func DeleteBatchData() throws {
        let database = try SQLiteBenchmarkRunner.GetConnection()
        
        let num_trips = try GetNumSQLiteRecords(sqlite_connection: database)
        try SQLiteBenchmarkRunner.deleteAllTrips(sqlite_connection: database)
        
        print("deleted \(num_trips)")
    }
    
    func DeleteSingleData() throws {
        
    }

    
}
