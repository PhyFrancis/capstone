import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.streaming._

final val scheme = StructType(Array(
  StructField("YEAR", NumericType),
  StructField("MONTH", NumericType),
  StructField("DAY_OF_MONTH", NumericType),
  StructField("DAY_OF_WEEK", NumericType),
  StructField("FLIGHT_DATE", DateType),
  StructField("UNIQUE_CARRIER", StringType),
  StructField("AIRLINE_ID", NumericType),
  StructField("CARRIER", StringType),
  StructField("FLIGHT_NUM", StringType),
  StructField("ORIGIN", StringType),
  StructField("DEST", StringType),
  StructField("CRS_DEP_TIME", StringType),
  StructField("DEP_DELAY", NumericType),
  StructField("CRS_ARR_TIME", StringType),
  StructField("ARR_DELAY", NumericType),
  StructField("CANCELLED", NumericType),
  StructField("DIVERTED", NumericType),
  StructField("FLIGHTS", NumericType)
));

object G1Q1 {
  def main(args: Array[String]) {
    val inputPath = args(0)
    val outputPath = args(1)

    val spark = SparkSession.builder
        .appName("G1Q1")
        .getOrCreate()

    val counts = spark.reaadStream(inputPath)
	.schema(schema)
	.map(_.split(":::"))
	.flatMap(fields => Array((fields(9),1), (fields(10),1)))
	.reduceByKey(_ + _)
	.count()

    val query = counts.writeStream.outputMode("complete")
        .format("console")
        .start()
    query.awaitTermination()
  }
}
