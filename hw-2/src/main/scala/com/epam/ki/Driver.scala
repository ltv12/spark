package com.epam.ki

import com.epam.ki.commands.QueryCommands
import com.epam.ki.commands.QueryCommands._
import com.epam.ki.processors.QueryProcessor
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by Lev_Khacheresiantc on 8/15/2016.
  */


object Driver {

  def main(args: Array[String]) {
    require(args.length == 1, s"Command is missing. Use one of them: $QueryCommands.")

    val command = args(0).toLowerCase()
    require(QueryCommands.commands.contains(command), s"It is possible to use only this commands: $QueryCommands.")

    val conf = new SparkConf().setAppName("Access Log Parser")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val flights = sqlContext.sql("SELECT year, month, uniquecarrier, dest, origin FROM hw_4.flights_orc")
    val airports = sqlContext.sql("SELECT iata, city, airport FROM hw_4.airports_orc")
    val carriers = sqlContext.table("hw_4.carriers_orc")

    val processor = new QueryProcessor(sqlContext, flights, airports, carriers)

    command match {
      case QUERY_1 => processor.query1.show()
      case QUERY_2 => println(processor.query2)
      case QUERY_3 => processor.query3(5).show()
      case QUERY_4 => processor.query4.show()
    }
  }
}

