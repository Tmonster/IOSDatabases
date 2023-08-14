//
//  DuckDBView.swift
//  IOSDatabases
//
//  Created by Tom Ebergen on 8/10/23.
//

import SwiftUI

struct DataBaseBenchmarkView: View {
    let title: String
    let database: DataBase
    
    
    
    var body: some View {
        Text(database.rawValue)
        VStack {
            Button("Import Data test") {
                ImportBatch()
            }.padding()
            Button("Import single data test") {
                ImportSingle()
            }.padding()
            Button("Update batch test") {
                UpdateBatch()
            }.padding()
            Button("Update single test") {
                UpdateSingle()
            }.padding()
            Button("Delete batch test") {
                DeleteBatch()
            }.padding()
            Button("Delete single test") {
                DeleteSingle()
            }.padding()
            Button("Queries") {
            }.padding()
        }
    }
    
    func ImportBatch() {
        do {
            switch database {
            case .CoreData:
                print("core data not implemented yet")
            case .DuckDB:
                try DuckDBBenchmarkRunner.ImportBatchData()
            case .RealmDB:
                try RealmBenchmarkRunner.ImportBatchData()
            case .SQLite:
                print("not implemented yet")
//                SQLLiteManager.ImportData()
            }
        } catch {
            print("error calling import Data \(error)")
        }
    }
    func ImportSingle() {
        
    }
    func UpdateBatch() {
        
    }
    func UpdateSingle() {
        
    }
    func DeleteBatch() {
        
    }
    func DeleteSingle() {
        
    }
    
}

struct DuckDBView_Previews: PreviewProvider {
    static var previews: some View {
        DataBaseBenchmarkView(title: "DuckDB view", database: DataBase.DuckDB)
    }
}
