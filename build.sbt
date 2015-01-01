name := "Player Analysis"

version := "0.0.1"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-streaming" % "0.9.0-incubating",
  "org.apache.spark" %% "spark-streaming-twitter" % "0.9.0-incubating",
  "net.sf.opencsv" % "opencsv" % "2.0"
)