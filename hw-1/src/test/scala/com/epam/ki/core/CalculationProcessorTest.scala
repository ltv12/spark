package com.epam.ki.core

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Assert._
import org.scalatest.{BeforeAndAfter, FunSuite}

/**
  * Created by Lev_Khacheresiantc on 8/23/2016.
  */
class CalculationProcessorTest extends FunSuite with BeforeAndAfter {

  var conf: SparkConf = new SparkConf().setAppName("scala-test").setMaster("local")
  var sc: SparkContext = new SparkContext(conf)
  var processor: CalculationProcessor = new CalculationProcessor

  test("checks transformation and statistics") {
    val listDataToTest = Seq("ip1 - - [28/Apr/2011:02:17:18 -0400] \"GET a.html HTTP/1.1\" 200 500 \"-\" \"ichiro/4.0 (http://help.goo.ne.jp/door/crawler.html)\"",
      "ip2 - - [28/Apr/2011:02:19:43 -0400] \"GET a.html HTTP/1.1\" 300 - \"-\" \"Mozilla/5.0 (Windows NT 6.1; WOW64; rv:2.0) Gecko/20100101 Firefox/4.0\"",
      "ip3 - - [28/Apr/2011:00:39:37 -0400] \"GET x.gif HTTP/1.1\" 200 1500 \"-\" \"Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/534.16 (KHTML, like Gecko) Chrome/10.0.648.205 Safari/534.16\""
    )

    val rdd = sc.parallelize(listDataToTest)
    val actualStatistics = processor.initStatistics(sc)
    val actualResult = processor.parseLogsWithStatistics(rdd, actualStatistics).collect()

    val expectedStatistics = Map("Internet Explorer" -> 0, "Firefox" -> 1, "Others" -> 2)
    val expectedResult = Array(("ip1", 500), ("ip2", 0), ("ip3", 1500))

    assertEquals(actualResult(0), expectedResult(0))
    assertEquals(actualResult(1), expectedResult(1))
    assertEquals(actualResult(2), expectedResult(2))
    assertEquals(actualStatistics.toString, expectedStatistics.toString)
  }

  test("checks calculation") {
    val dataForTest = sc.parallelize(Seq(("ip1", 500L), ("ip1", 100L), ("ip2", 0L), ("ip3", 1500L), ("ip3", 1500L)))
    val averageAndTotalByIp = processor.calculateAverageAndTotalBytes(dataForTest).collect()

    assertEquals(averageAndTotalByIp(0), ("ip3", (3000L, 1500.00)))
    assertEquals(averageAndTotalByIp(1), ("ip1", (600L, 300.00)))
    assertEquals(averageAndTotalByIp(2), ("ip2", (0L, 0.00)))
  }
}
