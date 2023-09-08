//
//  CSVTripReader.swift
//  IOSDatabases
//
//  Created by Tom Ebergen on 14/08/2023.
//

import Foundation


final class BenchmarkManager {
    
    static let benchmark : Benchmark = Benchmark.spotify_benchmark
    
    static let NUM_TRIPS = 500016
    static let NYC_TAXI_CSV = "/Users/tomebergen/IOSDatabases/taxi-subset-all-months.csv"
    
    static let LINEITEM_CSV = "/Users/tomebergen/IOSDatabases/lineitem-bigger.csv"
    static let NUM_ITEMS = 1800736
    
    static let STREAMS_CSV = "/Users/tomebergen/IOSDatabases/spotify_streams.csv"
    static let NUM_STREAMS = 11813
    
    static func getNumCSVRecords() -> Int {
        switch benchmark {
        case .Taxi_benchmark:
            return NUM_TRIPS
        case .tpch_benchmark:
            return NUM_ITEMS
        case .spotify_benchmark:
            return NUM_STREAMS
        }
    }
    
    static func getTableName() -> String {
        switch benchmark {
        case .Taxi_benchmark:
            return "trips"
        case .tpch_benchmark:
            return "lineitem"
        case .spotify_benchmark:
            return "streaming_history"
        }
    }
    
    static func getAggregateQuery1() -> String {
        switch benchmark {
        case .Taxi_benchmark:
            return AggregateQuery.taxi_aggregate_q01.rawValue
        case .tpch_benchmark:
            return AggregateQuery.tpch_q01.rawValue
        case .spotify_benchmark:
            return AggregateQuery.spotify_aggregate_q01.rawValue
        }
    }
    
    static func getAggregateQuery2() -> String {
        switch benchmark {
        case .Taxi_benchmark:
            return AggregateQuery.taxi_aggregate_q02.rawValue
        case .tpch_benchmark:
            return AggregateQuery.tpch_q02.rawValue
        case .spotify_benchmark:
            return AggregateQuery.spotify_aggregate_q02.rawValue
        }
    }
    
    static func getAggregateQuery3() -> String {
        switch benchmark {
        case .Taxi_benchmark:
            return AggregateQuery.taxi_aggregate_q03.rawValue
        case .tpch_benchmark:
            return AggregateQuery.tpch_q03.rawValue
        case .spotify_benchmark:
            return AggregateQuery.spotify_aggregate_q03.rawValue
        }
    }
    
    static func getCSVFile() -> String {
        switch benchmark {
        case .Taxi_benchmark:
            return NYC_TAXI_CSV
        case .tpch_benchmark:
            return LINEITEM_CSV
        case .spotify_benchmark:
            return STREAMS_CSV
        }
    }
    
    static func readCSV() async -> [String] {
        // split the filename
        var file : String
        switch benchmark {
        case .Taxi_benchmark:
            file = NYC_TAXI_CSV
        case .tpch_benchmark:
            file = LINEITEM_CSV
        case .spotify_benchmark:
            do {
                let content = try await String(contentsOf: DataManager.getDataURL())
                let parsedCSV: [String] = content.components(separatedBy: "\n")
                return parsedCSV
            } catch {
                print("whoops")
            }
            print("should never get here!!!")
            exit(1)
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
