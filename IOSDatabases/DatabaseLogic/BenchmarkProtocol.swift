//
//  BenchmarkMom.swift
//  IOSDatabases
//
//  Created by Tom Ebergen on 14/08/2023.
//

import Foundation

// benchmark protocol. Every benchmark must run this sort of function.
protocol BenchmarkProtocol {

    
    
    static func ImportBatchData() throws
    static func ImportSingleData() throws
    func UpdateBatchData() throws
    func UpdateSingleData() throws
    static func DeleteBatchData() throws
    func DeleteSingleData() throws
    static func RunAggregateQuery() throws
}
