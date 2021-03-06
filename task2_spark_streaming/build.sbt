name := "CapstoneTask2"

version := "1.0"

scalaVersion := "2.11.8"

mergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  case _                                                   => MergeStrategy.first
}

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "2.2.1" % "provided",
  "org.apache.spark" %% "spark-streaming" % "2.2.1" % "provided",
  "org.apache.spark" %% "spark-streaming-kafka" % "1.6.2",
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.2"
)
