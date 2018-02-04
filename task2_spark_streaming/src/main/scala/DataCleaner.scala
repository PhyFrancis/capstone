import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

// ========= Raw data schema ==========
// Delimiter is ":::"
//0   StructField("YEAR", NumericType),
//1   StructField("MONTH", NumericType),
//2   StructField("DAY_OF_MONTH", NumericType),
//3   StructField("DAY_OF_WEEK", NumericType),
//4   StructField("FLIGHT_DATE", DateType),
//5   StructField("UNIQUE_CARRIER", StringType),
//6   StructField("AIRLINE_ID", NumericType),
//7   StructField("CARRIER", StringType),
//8   StructField("FLIGHT_NUM", StringType),
//9   StructField("ORIGIN", StringType),
//10  StructField("DEST", StringType),
//11  StructField("CRS_DEP_TIME", StringType),
//12  StructField("DEP_DELAY", NumericType),
//13  StructField("CRS_ARR_TIME", StringType),
//14  StructField("ARR_DELAY", NumericType),
//15  StructField("CANCELLED", NumericType),
//16  StructField("DIVERTED", NumericType),
//17  StructField("FLIGHTS", NumericType)

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

object DataCleaner {
  def main(args: Array[String])  {
    val inputPath = args(0)
    val outputPath = args(1)

    val conf = new SparkConf().setAppName("data cleaner")
    val sc = new SparkContext(conf)

    val selectIndex = Array(4,5,8,9,10,11,12,13,14,15,16)
    sc.textFile(inputPath)
      .map{line =>
        val fields = line.split(":::")
        selectIndex.map(i => fields(i)).mkString("|").replace("\"","")
      }.saveAsTextFile(outputPath)
  }
}
