//
//  ContentView.swift
//  IOSDatabases
//
//  Created by Tom Ebergen on 8/10/23.
//

import SwiftUI
import CoreData

struct ContentView: View {
    var body: some View {
        NavigationStack {
            ForEach(DataBase.allCases) { db in
                NavigationLink(db.rawValue, value: db)
            }
            .buttonStyle(.bordered)
            .navigationDestination(for: DataBase.self, destination: {
                database in
                switch database {
                case .DuckDB:
                    DataBaseBenchmarkView(title: database.rawValue, database: database)
                case .SQLite:
                    DataBaseBenchmarkView(title: database.rawValue, database: database)
                }
            })
        }
    }
}

struct ContentView_Previews: PreviewProvider {
    static var previews: some View {
        ContentView()
    }
}
