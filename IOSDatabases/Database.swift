//
//  Database.swift
//  IOSDatabases
//
//  Created by Tom Ebergen on 8/10/23.
//

import Foundation

enum DataBase : String, Hashable, CaseIterable, Identifiable {
    case DuckDB = "DuckDB"
    case CoreData = "CoreData"
    case RealmDB = "RealmDB"
    case SQLite = "SQLite"
    
    var id: String {
        return self.rawValue
    }
}

