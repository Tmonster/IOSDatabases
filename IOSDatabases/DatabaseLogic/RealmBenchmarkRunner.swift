//
//  DuckDBLogic.swift
//  IOSDatabases
//
//  Created by Tom Ebergen on 8/10/23.
//

import Foundation
import RealmSwift

class Trip : Object {
    @objc var vendor_name           : String = ""
    @objc var passenger_count       : Int = 0
    @objc var trip_distance         : Double = 0
    @objc var pickup_longitude      : Double = 0
    @objc var pickup_latitude       : Double = 0
    @objc var rate_code             : String = ""
    @objc var store_and_fwd         : String = ""
    @objc var dropoff_longitude     : Double = 0
    @objc var dropoff_latitude      : Double = 0
    @objc var payment_type          : String = ""
    @objc var fare_amount           : Double = 0
    @objc var extra                 : Double = 0
    @objc var mta_tax               : Double = 0
    @objc var tip_amount            : Double = 0
    @objc var tolls_amount          : Double = 0
    @objc var total_amount          : Double = 0
    @objc var improvement_surcharge : Double = 0
    @objc var congestion_surcharge  : Double = 0
    @objc var pickup_location_id    : Int = 0
    @objc var dropoff_location_id   : Int = 0
    @objc var year                  : String = ""
    @objc var month                 : String = ""
}

//final class RealmBenchmarkRunner : BenchmarkProtocol {
final class RealmBenchmarkRunner {
    
    static let realm_filename = "taxis"
    static let realm_extension = ".realmdb"
    
    static func GetRealmConnection() throws -> Realm {
        // Create our database and connection as described above
        
        var config = Realm.Configuration.defaultConfiguration
        config.deleteRealmIfMigrationNeeded = true
        config.fileURL!.deleteLastPathComponent()
        config.fileURL!.appendPathComponent(realm_filename)
        config.fileURL!.appendPathExtension(realm_extension)
        let realm_connection = try! Realm(configuration: config)
    
        return realm_connection
    }
    
    static func deleteAllTrips(realm_connection : Realm) {
        
        let allTrips = realm_connection.objects(Trip.self)
    
        try! realm_connection.write {
            realm_connection.delete(allTrips)
        }
    }
    
    
    static func parseTrip(str_trip: String) -> Trip {
        let trip = Trip()
        let parsed_row = str_trip.components(separatedBy: ",")
        trip.vendor_name           = String(parsed_row[0])
        //            trip.pickup_datetime       = Time.init(Date(parsed_row[1]))
        //            trip.dropoff_datetime      = Time.init(Date(parsed_row[2]))
        trip.passenger_count       = Int(parsed_row[3]) ?? 0
        trip.trip_distance         = Double(parsed_row[4]) ?? 0
        trip.pickup_longitude      = Double(parsed_row[5]) ?? 0
        trip.pickup_latitude       = Double(parsed_row[6]) ?? 0
        trip.rate_code             = String(parsed_row[7])
        trip.store_and_fwd         = String(parsed_row[8])
        trip.dropoff_longitude     = Double(parsed_row[9]) ?? 0
        trip.dropoff_latitude      = Double(parsed_row[10]) ?? 0
        trip.payment_type          = String(parsed_row[11])
        trip.fare_amount           = Double(parsed_row[12]) ?? 0
        trip.extra                 = Double(parsed_row[13]) ?? 0
        trip.mta_tax               = Double(parsed_row[14]) ?? 0
        trip.tip_amount            = Double(parsed_row[15]) ?? 0
        trip.tolls_amount          = Double(parsed_row[16]) ?? 0
        trip.total_amount          = Double(parsed_row[17]) ?? 0
        trip.improvement_surcharge = Double(parsed_row[18]) ?? 0
        trip.congestion_surcharge  = Double(parsed_row[19]) ?? 0
        trip.pickup_location_id    = Int(parsed_row[20]) ?? 0
        trip.dropoff_location_id   = Int(parsed_row[21]) ?? 0
        trip.year                  = String(parsed_row[22])
        trip.month                 = String(parsed_row[23])
        return trip
    }
    
    static func ImportBatchData() throws {
        Task {
            var parsed_data : [Trip] = []
            let all_data = await BenchmarkManager.readCSV()
            for row in all_data {
                if (row.count > 0) {
                    let trip = parseTrip(str_trip: row)
                    parsed_data.append(trip)
                }
            }
            
            // Setup Realm - Delete all previous trips
            do {
                // add the objects
                let realm_connection = try RealmBenchmarkRunner.GetRealmConnection()
                
                // could lead to bias, hopefully no.
                let allTrips = realm_connection.objects(Trip.self)
                if (allTrips.count > 0) {
                    RealmBenchmarkRunner.deleteAllTrips(realm_connection: realm_connection)
                    throw BenchmarkError.databaseNotEmpty(reason: "realm benchmark assumes database with no trips. All trips have been deleted, please re-run benchmark.")
                }
                
                try realm_connection.write {
                    realm_connection.add(parsed_data)
                }
                // delete the objects as well
                let num_trips = RealmBenchmarkRunner.GetNumtrips()
                if (num_trips != BenchmarkManager.NUM_TRIPS) {
                    throw BenchmarkError.realmBatchLoadCountMismatch
                }
                print("inserted \(num_trips) records into realm DB")
                
            }
        }
    }

    static func GetNumtrips() -> Int {
        var ret = 0
        do {
            let realm_connection = try RealmBenchmarkRunner.GetRealmConnection()
            let allTrips = realm_connection.objects(Trip.self)
            ret = allTrips.count
        } catch {
            print("encountered realm benchmark error \(error)")
        }
        return ret
    }
    
    
    static func RunAggregateQuery() throws {
    }

    

    
    static func DeleteBatchData() throws {
        let realm_connection = try GetRealmConnection()
        let num_trips = GetNumtrips()
        deleteAllTrips(realm_connection: realm_connection)
        print("deleted \(num_trips) from realm db")
        
    }
    
//    static func ImportSingleData() throws {}
//    func UpdateBatchData() throws {}
//    func UpdateSingleData() throws {}
//    func DeleteSingleData() throws {}
}
