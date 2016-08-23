package com.epam.ki.core

import com.epam.ki.utils.ParserUtils
import org.apache.spark.{Accumulator, SparkConf, SparkContext, rdd}
import org.apache.spark.rdd.RDD

/**
  * Created by Lev_Khacheresiantc on 8/23/2016.
  */
class CalculationProcessor {

  def calculateAverageAndTotalBytes(rdd: RDD[(String, Long)]): RDD[(String, (Double, Long))] = {
    rdd.aggregateByKey((0L, 0))(
      (acc, v) => (acc._1 + v, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    ).mapValues { case (total, size) => (total / size.toDouble, total) }
      .sortBy({ case (_, (_, total)) => total }, ascending = false)
  }

  def parseLogsWithStatistics(data: RDD[String], statistics: Map[String, Accumulator[Int]]): RDD[(String, Long)] = {
    data.map(ParserUtils.parseLog(_, statistics))
  }

  def initStatistics(sc: SparkContext): Map[String, Accumulator[Int]] = {

    val others = sc.accumulator(0)
    Map[String, Accumulator[Int]](
      ("Internet Explorer", sc.accumulator(0)),
      ("Firefox", sc.accumulator(0)),
      ("Others", others)
    ).withDefaultValue(others)
  }


}
