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
    case dataBaseImportError
    
    case databaseDeleteError(reason: String?)
    
    case realmBatchLoadCountMismatch
    case databaseNotEmpty(reason: String?)
}
