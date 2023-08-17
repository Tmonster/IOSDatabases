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
//            Button("Update batch test") {
//                UpdateBatch()
//            }.padding()
//            Button("Update single test") {
//                UpdateSingle()
//            }.padding()
            Button("Delete batch test") {
                DeleteBatch()
            }.padding()
//            Button("Delete single test") {
//                DeleteSingle()
//            }.padding()
            Button("Run Aggregate Query") {
                RunAggregateQuery()
            }.padding()
        }
    }
    
    func ImportBatch() {
        let start = DispatchTime.now()
        do {
            switch database {
            case .CoreData:
                print("core data not implemented yet")
            case .DuckDB:
                try DuckDBBenchmarkRunner.ImportBatchData()
            case .RealmDB:
                try RealmBenchmarkRunner.ImportBatchData()
            case .SQLite:
                try SQLiteBenchmarkRunner.ImportBatchData()
            }
        } catch {
            print("error calling import Data \(error)")
        }
        let end = DispatchTime.now()
        let nanoTime = end.uptimeNanoseconds - start.uptimeNanoseconds
        let timeInterval = Double(nanoTime) / 1_000_000_000
        print("\(database.rawValue): time to batch insert = \(timeInterval)")
    }
    func ImportSingle() {
        
    }
    func UpdateBatch() {
        
    }
    func UpdateSingle() {
        
    }
    func RunAggregateQuery() {
        let start = DispatchTime.now()
        do {
            switch database {
            case .CoreData:
                print("core data not implemented yet")
            case .DuckDB:
                try DuckDBBenchmarkRunner.RunAggregateQuery()
            case .RealmDB:
                try RealmBenchmarkRunner.RunAggregateQuery()
            case .SQLite:
                try SQLiteBenchmarkRunner.RunAggregateQuery()
            }
        } catch {
            print("error running query \(error)")
        }
        let end = DispatchTime.now()
        let nanoTime = end.uptimeNanoseconds - start.uptimeNanoseconds
        let timeInterval = Double(nanoTime) / 1_000_000_000
        print("\(database.rawValue): time to execute query = \(timeInterval)")
    }
    func DeleteBatch() {
        let start = DispatchTime.now()
        do {
            switch database {
            case .CoreData:
                print("core data not implemented yet")
            case .DuckDB:
                try DuckDBBenchmarkRunner.DeleteBatchData()
            case .RealmDB:
                try RealmBenchmarkRunner.DeleteBatchData()
            case .SQLite:
                try SQLiteBenchmarkRunner.DeleteBatchData()
            }
        } catch {
            print("error calling import Data \(error)")
        }
        let end = DispatchTime.now()
        let nanoTime = end.uptimeNanoseconds - start.uptimeNanoseconds
        let timeInterval = Double(nanoTime) / 1_000_000_000
        print("\(database.rawValue): time to batch delete = \(timeInterval)")
    }
    func DeleteSingle() {
        
    }
    
}

struct DuckDBView_Previews: PreviewProvider {
    static var previews: some View {
        DataBaseBenchmarkView(title: "DuckDB view", database: DataBase.DuckDB)
    }
}
