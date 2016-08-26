package com.epam.ki.processors

import org.apache.spark.sql.{DataFrame, Row, SQLContext}

/**
  * Created by Lev_Khacheresiantc on 8/24/2016.
  */
class QueryProcessor(sqlContext: SQLContext, flights: DataFrame, airports: DataFrame, carriers: DataFrame) {

  import sqlContext.implicits._

  def query1: DataFrame = {

    flights.select("uniquecarrier")
      .groupBy("uniquecarrier")
      .count()
      .orderBy($"count".desc)
  }

  def query2: Long = {
    flights.select("year", "month", "dest", "origin")
      .where($"month" === 6 and $"year" === 2007)
      .join(airports.where($"city" === "New York"),
        $"origin" === $"iata" or $"dest" === $"iata")
      .select("airport")
      .count()
  }

  //  == Physical Plan ==
  //  Project [airport#30,count#124L]
  //  +- BroadcastHashJoin [iata#122], [iata#29], BuildRight
  //  :- ConvertToUnsafe
  //    :  +- TakeOrderedAndProject(limit=5, orderBy=[count#124L DESC], output=[iata#122,count#124L])
  //  :     +- ConvertToSafe
  //    :        +- TungstenAggregate(key=[iata#122], functions=[(count(1),mode=Final,isDistinct=false)], output=[iata#122,count#124L])
  //  :           +- TungstenExchange hashpartitioning(iata#122,200), None
  //    :              +- TungstenAggregate(key=[iata#122], functions=[(count(1),mode=Partial,isDistinct=false)], output=[iata#122,count#178L])
  //  :                 +- Union
  //    :                    :- Project [dest#17 AS iata#122]
  //  :                    :  +- InMemoryColumnarTableScan [dest#17], InMemoryRelation [year#0,month#1,uniquecarrier#8,dest#17,origin#16], true, 10000, StorageLevel(true, true, false, true, 1), ConvertToUnsafe, None
  //    :                    +- Project [origin#16 AS iata#123]
  //  :                       +- InMemoryColumnarTableScan [origin#16], InMemoryRelation [year#0,month#1,uniquecarrier#8,dest#17,origin#16], true, 10000, StorageLevel(true, true, false, true, 1), ConvertToUnsafe, None
  //  +- ConvertToUnsafe
  //    +- HiveTableScan [iata#29,airport#30], MetastoreRelation hw_4, airports_orc, None


  def query3(topNumber: Int): DataFrame = {

    val filteredFlights = flights.where($"month" between(6, 8))

    filteredFlights.select($"dest".as("iata"))
      .unionAll(filteredFlights.select($"origin".as("iata")))
      .groupBy($"iata")
      .count()
      .sort($"count".desc)
      .limit(topNumber)
      .join(airports.select($"iata", $"airport"), "iata")
      .select($"airport", $"count")
  }

  //  == Physical Plan ==
  //    Project [airport#30,count#184L]
  //  +- BroadcastHashJoin [iata#183], [iata#29], BuildRight
  //  :- ConvertToUnsafe
  //    :  +- TakeOrderedAndProject(limit=5, orderBy=[count#184L DESC], output=[iata#183,count#184L])
  //  :     +- ConvertToSafe
  //    :        +- TungstenAggregate(key=[iata#183], functions=[(count(1),mode=Final,isDistinct=false)], output=[iata#183,count#184L])
  //  :           +- TungstenExchange hashpartitioning(iata#183,200), None
  //    :              +- TungstenAggregate(key=[iata#183], functions=[(count(1),mode=Partial,isDistinct=false)], output=[iata#183,count#188L])
  //  :                 +- !Generate explode(array(origin#16,dest#17)), false, false, [iata#183]
  //  :                    +- ConvertToSafe
  //    :                       +- Project [origin#16,dest#17]
  //  :                          +- Filter ((cast(month#1 as int) >= 6) && (cast(month#1 as int) <= 8))
  //  :                             +- HiveTableScan [origin#16,dest#17,month#1], MetastoreRelation hw_4, flights_orc, None
  //  +- ConvertToUnsafe
  //    +- HiveTableScan [iata#29,airport#30], MetastoreRelation hw_4, airports_orc, None

  def query3Alternative(topNumber: Int): DataFrame = {

    import org.apache.spark.sql.functions._

    flights.select($"month", explode(array($"origin", $"dest")).as("iata"))
      .where($"month" between(6, 8))
      .groupBy($"iata")
      .count()
      .sort($"count".desc)
      .limit(topNumber)
      .join(airports.select("iata", "airport"), "iata")
      .select($"airport", $"count")
  }


  def query4: DataFrame = {
    flights.select("uniquecarrier")
      .join(carriers, $"uniquecarrier" === $"code")
      .groupBy("description")
      .count()
      .sort($"count".desc)
      .limit(1)
  }
}

