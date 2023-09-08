//
//  Database.swift
//  IOSDatabases
//
//  Created by Tom Ebergen on 8/10/23.
//

import Foundation

enum DataBase : String, Hashable, CaseIterable, Identifiable {
    case DuckDB = "DuckDB"
//    case CoreData = "CoreData"
//    case RealmDB = "RealmDB"
    case SQLite = "SQLite"
    
    var id: String {
        return self.rawValue
    }
}


enum Benchmark : String, Hashable, CaseIterable, Identifiable {
    case Taxi_benchmark = "nyc_taxi"
    case tpch_benchmark = "tpch"
    case spotify_benchmark = "spotify_benchmark"
    
    var id: String {
        return self.rawValue
    }
}


enum AggregateQuery : String, Identifiable {
    
case tpch_q01 = "SELECT l_returnflag, l_linestatus, sum(l_quantity) AS sum_qty, sum(l_extendedprice) AS sum_base_price, sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, avg(l_quantity) AS avg_qty, avg(l_extendedprice) AS avg_price,     avg(l_discount) AS avg_disc, count(*) AS count_order FROM lineitem WHERE l_shipdate >= CAST('1998-09-02' AS date) GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus;"
case tpch_q02 = "SELECT 1"
case tpch_q03 = "SELECT 2"
    
case taxi_aggregate_q01 = "Select avg(tip_amount/total_amount) * 100, month from trips group by month"
case taxi_aggregate_q02 = "Select count(*), month trips where tip_amount = 0 group by month"
case taxi_aggregate_q03 = "Select max(tip_amount) form trips group by month"
    
case spotify_aggregate_q01 = "select artist_name, count(*) as num_listens from streaming_history group by artist_name order by num_listens desc limit 20;"
case spotify_aggregate_q02 = "select track_name, count(*) as num_listens from streaming_history group by track_name, artist_name order by num_listens desc limit 20;"
case spotify_aggregate_q03 = "select ((sum(ms_played)/1000)/60)::INTEGER as min_played, track_name from streaming_history group by track_name order by min_played desc limit 20;"
    
    var id: String {
        return self.rawValue
    }
 
}
