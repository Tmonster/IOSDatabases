//
//  CSVTripReader.swift
//  IOSDatabases
//
//  Created by Tom Ebergen on 14/08/2023.
//

import Foundation


final class CSVTripReader {
    
    static let NUM_TRIPS = 999996
    
    static let CSV_FILE = "/Users/tomebergen/IOSDatabases/taxi-subset-all-months.csv"
    
    static let NUM_ITEMS = 500000
    static let LINEITEM_FILE = "/Users/tomebergen/IOSDatabases/lineitem-small.csv"
    
    static func readCSV(inputFile: String, separator: String) -> [String] {
        // split the filename
        do {
            let content = try String(contentsOfFile: inputFile)
            let parsedCSV: [String] = content.components(separatedBy: "\n")
            return parsedCSV
        }
        catch {
            return []
        }
    }
    
}
