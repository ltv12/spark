package com.epam.ki.commands

/**
  * Created by Lev_Khacheresiantc on 8/24/2016.
  */
object QueryCommands {
  val QUERY_1 = "query1"
  val QUERY_2 = "query2"
  val QUERY_3 = "query3"
  val QUERY_4 = "query4"

  val commands = Seq(QUERY_1, QUERY_2, QUERY_3, QUERY_4)

  override def toString: String = {
    commands.mkString(", ")
  }
}
