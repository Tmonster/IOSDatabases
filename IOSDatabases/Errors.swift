//
//  Errors.swift
//  IOSDatabases
//
//  Created by Tom Ebergen on 14/08/2023.
//

import Foundation


public enum BenchmarkError: Error {
  /// for some reason cannot load the database
    case dataBaseLoadError
    /// when importing data, the number of records imported does not match
    /// the number of records in the CSV file
    case dataBaseImportError(reason: String?)
    
    case databaseDeleteError(reason: String?)
    
    case realmBatchLoadCountMismatch
    case databaseNotEmpty(reason: String?)
}
