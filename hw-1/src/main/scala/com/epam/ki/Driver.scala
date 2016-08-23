package com.epam.ki

import java.io.Serializable

import com.epam.ki.core.CalculationProcessor
import com.epam.ki.utils.ParserUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{Accumulator, SparkConf, SparkContext}


/**
  * Created by Lev_Khacheresiantc on 8/15/2016.
  */


object Driver {
  def main(args: Array[String]) {
    if (args.length != 2) throw new IllegalArgumentException("You have to specify output and input directories")

    val conf = new SparkConf().setAppName("Access Log Parser")
    val sc = new SparkContext(conf)
    val processor = new CalculationProcessor

    val sourceFile = args(0)
    val targetFile = args(1)

    val data = sc.textFile(sourceFile)

    val browserStatistic = processor.initStatistics(sc)
    val parsedLogs = processor.parseLogsWithStatistics(data, browserStatistic)
    val averageAndTotalByIp = processor.calculateAverageAndTotalBytes(parsedLogs)

    averageAndTotalByIp.take(5).foreach(println)
    averageAndTotalByIp
      .map({ case (ip, (size, avr)) => Array(ip, size, avr).mkString(",") })
      .saveAsTextFile(s"${targetFile}_result")
    browserStatistic.foreach(println)

  }


}

