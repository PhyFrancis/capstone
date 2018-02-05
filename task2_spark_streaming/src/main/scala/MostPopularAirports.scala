package capstone

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.streaming._

import capstone.Common.schema
import cassandra.CassandraForeachSink

// ========== CLeaned data schema ==========
// Delimiter is "|"
//    StructField("FLIGHT_DATE", DateType),
//    StructField("UNIQUE_CARRIER", StringType),
//    StructField("FLIGHT_NUM", StringType),
//    StructField("ORIGIN", StringType),
//    StructField("DEST", StringType),
//    StructField("CRS_DEP_TIME", StringType),
//    StructField("DEP_DELAY", NumericType),
//    StructField("CRS_ARR_TIME", StringType),
//    StructField("ARR_DELAY", NumericType),
//    StructField("CANCELLED", NumericType),
//    StructField("DIVERTED", NumericType),

object G1Q1 {
  def main(args: Array[String]) {
    val inputPath = args(0)
    val outputPath = args(1)

    val spark = SparkSession.builder
        .appName("G1Q1")
        .getOrCreate()
    val rowDf = spark.readStream
        .option("sep", "|")
        .schema(schema)
        .csv(inputPath)

    import spark.implicits._
    val query = rowDf
        .flatMap(r => Array(r.getString(3), r.getString(4)))
        .withColumn("timestamp", current_timestamp)
        .withWatermark("timestamp", "10 seconds")
        .groupBy(
            window($"timestamp", "10 seconds", "10 seconds"),
            $"value")
        .count()
        .writeStream
        // .outputMode("append")
        // .format("console")
        .foreach(new CassandraForeachSink()) // TODO should use 'update' mode instead of 'append'
        .start()
    query.awaitTermination()
  }
}
