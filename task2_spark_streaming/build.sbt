name := "CapstoneTask2"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "2.2.1" % "provided",
  "org.apache.spark" %% "spark-streaming" % "2.2.1" % "provided",
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.2"
)
