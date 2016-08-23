package com.epam.ki.utils

import java.io.Serializable

import eu.bitwalker.useragentutils._
import org.apache.spark.Accumulator

import scala.collection.immutable.HashMap

/**
  * Created by Lev_Khacheresiantc on 8/19/2016.
  */
object ParserUtils {

  val IP = "(ip[\\d]*)"
  val DATE = "(?:\\d{2}/[A-Z]{1}[a-z]{2}/\\d{4}:\\d{2}:\\d{2}:\\d{2} -\\d{4})"
  val METHOD = "(?:GET|POST|PUT|DELETE|HEAD|OPTIONS|TRACE|CONNECT)"
  val URL_PATH = "(?:\\S*)"
  val PROTOCOL = "(?:\\S*)"
  val CODE = "(?:\\d{3})"
  val BYTES = "(\\S*)"
  val URL = "(?:.*)"
  val USER_AGENT = "(.*)"

  private val LOG_TEPMLATE = s"""$IP - - \\[$DATE\\] \"$METHOD $URL_PATH $PROTOCOL\" $CODE $BYTES \"$URL\" \"$USER_AGENT\"""".r


  def getBrowser(agentInfo: String): String = {
    UserAgent.parseUserAgentString(agentInfo).getBrowser.getGroup.getName
  }


  def parseLog(line: String, counters: Map[String, Accumulator[Int]]): (String, Long) = {
       val LOG_TEPMLATE(ip, size, agent) = line
       counters(getBrowser(agent)) += 1
       size match {
         case "-" => (ip, 0)
         case _ => (ip, size.toLong)
       }
  }
}
