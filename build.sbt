name := "HistogramBinGeneration"

version := "0.1"

// scalaVersion := "2.13.3"
scalaVersion := "2.12.8"

val SparkVersion = "3.0.0"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.2.0" % "test",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2"
)
