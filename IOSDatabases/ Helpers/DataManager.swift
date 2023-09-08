//
//  DataManager.swift
//  IOSDatabases
//
//  Created by Tom Ebergen on 05/09/2023.
//

import Foundation
import DuckDB

final class DataManager {
    
    static let spotify_streams_url = "https://raw.githubusercontent.com/Tmonster/IOSDatabases/spotify-example/spotify_streams.csv"
    
    static func getDataURL() async throws -> URL {
        let (csvFileURL, _) = try await URLSession.shared.download(
                from: URL(string: spotify_streams_url)!)
        return csvFileURL
    }
}
