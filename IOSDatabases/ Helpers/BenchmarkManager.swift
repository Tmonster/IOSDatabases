//
//  CSVTripReader.swift
//  IOSDatabases
//
//  Created by Tom Ebergen on 14/08/2023.
//

import Foundation


final class BenchmarkManager {
    
    static let benchmark : Benchmark = Benchmark.tpch_benchmark
    
    static let NUM_TRIPS = 500016
    static let NYC_TAXI_CSV = "/Users/tomebergen/IOSDatabases/taxi-subset-all-months.csv"
    
    static let LINEITEM_CSV = "/Users/tomebergen/IOSDatabases/lineitem-bigger.csv"
    static let NUM_ITEMS = 1800736
    
    static func getNumCSVRecords() -> Int {
        switch benchmark {
        case .Taxi_benchmark:
            return NUM_TRIPS
        case .tpch_benchmark:
            return NUM_ITEMS
        }
    }
    
    static func getTableName() -> String {
        switch benchmark {
        case .Taxi_benchmark:
            return "Trips"
        case .tpch_benchmark:
            return "Lineitem"
        }
    }
    
    static func getAggregateQuery() -> String {
        switch benchmark {
        case .Taxi_benchmark:
            return AggregateQuery.taxi_aggregate_q01.rawValue
        case .tpch_benchmark:
            return AggregateQuery.tpch_q01.rawValue
        }
    }
    
    static func getCSVFile() -> String {
        switch benchmark {
        case .Taxi_benchmark:
            return NYC_TAXI_CSV
        case .tpch_benchmark:
            return LINEITEM_CSV
        }
    }
    
    static func readCSV() -> [String] {
        // split the filename
        var file : String
        switch benchmark {
        case .Taxi_benchmark:
            file = NYC_TAXI_CSV
        case .tpch_benchmark:
            file = LINEITEM_CSV
        }
        do {
            let content = try String(contentsOfFile: file)
            let parsedCSV: [String] = content.components(separatedBy: "\n")
            return parsedCSV
        }
        catch {
            return []
        }
    }
    
}
