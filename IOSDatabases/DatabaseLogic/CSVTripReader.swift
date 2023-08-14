//
//  CSVTripReader.swift
//  IOSDatabases
//
//  Created by Tom Ebergen on 14/08/2023.
//

import Foundation


final class CSVTripReader {
    
    static let NUM_TRIPS = 50000
    
    static func readCSV(inputFile: String, separator: String) -> [String]{
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
