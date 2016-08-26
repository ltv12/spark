package com.epam.ki.processors

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}


/**
  * Created by Lev_Khacheresiantc on 8/24/2016.
  */
class QueryProcessorTest extends FunSuite with BeforeAndAfterAll {

  var processor: QueryProcessor = _


  override def beforeAll() = {
    val conf = new SparkConf().setAppName("test QueryProcessor").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val fligths = sqlContext.createDataFrame(
      Seq(
        Flight(2007, 1, "AA", "I1", "I2"),
        Flight(2007, 2, "AA", "I1", "I2"),
        Flight(2007, 2, "AA", "I2", "I1"),
        Flight(2007, 6, "AB", "I1", "I2"),
        Flight(2007, 8, "AB", "I2", "I3"),
        Flight(2007, 8, "AC", "I3", "I3"),
        Flight(2007, 9, "AC", "I3", "I3")
      )
    )
    val airports = sqlContext.createDataFrame(
      Seq(
        Airport("I1", "New York", "I1 Airport"),
        Airport("I2", "Los Angeles", "I2 Airport"),
        Airport("I3", "Boston", "I3 Airport")
      )
    )
    val carriers = sqlContext.createDataFrame(
      Seq(
        Carrier("AA", "AA Carrier"),
        Carrier("AB", "AB Carrier"),
        Carrier("AC", "AC Carrier")
      )
    )

    processor = new QueryProcessor(sqlContext, fligths, airports, carriers)
  }

  test("Count total number of flights per carrier in 2007") {
    val actualResult = processor.query1.collect()
    val expectedResult = Array(Row("AA", 3), Row("AB", 2), Row("AC", 2))

    assert(actualResult.sameElements(expectedResult))
  }

  test("The total number of flights served in Jun 2007 by NYC") {
    val actualResult = processor.query2
    val expectedResult = 1L
    assert(actualResult == expectedResult)
  }

  test("Find five most busy airports in US during Jun 01 - Aug 31") {
    val actualResult = processor.query3(2).collect()
    val expectedResult = Array(Row("I3 Airport", 3), Row("I2 Airport", 2))

    assert(actualResult.sameElements(expectedResult))
  }

  test("Find the carrier who served the biggest number of flights") {
    val actualResult = processor.query4.collect()
    val expectedResult = Array(Row("AA Carrier", 3))

    assert(actualResult.sameElements(expectedResult))
  }

}

case class Flight(year: Int, month: Int, uniquecarrier: String, dest: String, origin: String)

case class Airport(iata: String, city: String, airport: String)

case class Carrier(code: String, description: String)
