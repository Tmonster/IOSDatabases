//
//  DuckDBView.swift
//  IOSDatabases
//
//  Created by Tom Ebergen on 8/10/23.
//

import SwiftUI
import DuckDB

struct DataBaseBenchmarkView: View {
    let title: String
    let database: DataBase
    var body: some View {
        Text(database.rawValue)
        VStack {
            Button("Import Data test") {
                ImportBatch()
            }
            Button("Import single data test") {
                ImportSingle()
            }
            Button("Update batch test") {
                UpdateBatch()
            }
            Button("Update single test") {
                UpdateSingle()
            }
            Button("Delete batch test") {
                DeleteBatch()
            }
            Button("Delete single test") {
                DeleteSingle()
            }
            Button("Queries") {
            }
        }
    }
    
    func ImportBatch() {
        
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
