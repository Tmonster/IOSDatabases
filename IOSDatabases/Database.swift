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


enum Benchmark : String, Hashable, CaseIterable, Identifiable {
    case Taxi_benchmark = "nyc_taxi"
    case tpch_benchmark = "tpch"
    
    var id: String {
        return self.rawValue
    }
}


enum AggregateQuery : String, Identifiable {
case tpch_q01 = "SELECT     l_returnflag,     l_linestatus,     sum(l_quantity) AS sum_qty,     sum(l_extendedprice) AS sum_base_price,     sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,     sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,     avg(l_quantity) AS avg_qty,     avg(l_extendedprice) AS avg_price,     avg(l_discount) AS avg_disc,     count(*) AS count_order FROM     lineitem WHERE     l_shipdate >= CAST('1998-09-02' AS date) GROUP BY     l_returnflag,     l_linestatus ORDER BY     l_returnflag,     l_linestatus;"
case taxi_aggregate_q01 = "Select avg(tip_amount/total_amount) * 100, month from trips group by month"
    
    var id: String {
        return self.rawValue
    }
}
