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
    static func DeleteBatchData() throws
    static func RunAggregateQuery() throws
    //    static func ImportSingleData() throws
    //    func UpdateBatchData() throws
    //    func UpdateSingleData() throws
    //    func DeleteSingleData() throws
}
