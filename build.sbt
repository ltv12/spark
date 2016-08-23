import sbt.Keys._

lazy val commonSettings = Seq(
  version := "1.0",
  scalaVersion := "2.10.6",
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "1.6.0" % "provided",
    "org.apache.spark" %% "spark-sql" % "1.6.0" % "provided",
    "org.scalatest" %% "scalatest" % "1.9.1" % "test",
    "junit" % "junit" % "4.12" % "test",
    "com.novocode" % "junit-interface" % "0.11" % "test"
  )
)


lazy val hw1 = (project in file("hw-1"))
  .settings(commonSettings: _*)
  .settings(
    name := "spark-hw-1",
    libraryDependencies ++= Seq(
      "eu.bitwalker" % "UserAgentUtils" % "1.14" % "provided"
    )
    //mainClass in (Compile, run) := Some("com.epam.ki.test.HomeWork1")
  )

lazy val hw2 = (project in file("hw-2"))
  .settings(commonSettings: _*)
  .settings(
    name := "spark-hw-2"
    //mainClass in (Compile, run) := Some("com.epam.ki.test.HomeWork1")
  )

lazy val hw3 = (project in file("hw-3"))
  .settings(commonSettings: _*)
  .settings(
    name := "spark-hw-3"
    //mainClass in (Compile, run) := Some("com.epam.ki.test.HomeWork1")
  )

