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


  def query3(topNumber: Int): DataFrame = {

    val filteredFlights = flights.where($"month" between(6, 8))

    filteredFlights
      .join(airports, $"dest" === $"iata")
      .unionAll(filteredFlights.join(airports, $"origin" === $"iata"))
      .groupBy("airport")
      .count()
      .sort($"count".desc)
      .limit(topNumber)
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

