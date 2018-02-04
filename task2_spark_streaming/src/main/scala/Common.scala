package capstone

import org.apache.spark.sql.types._

object Common {
  def schema = StructType(Array(
    StructField("FLIGHT_DATE", DateType),
    StructField("UNIQUE_CARRIER", StringType),
    StructField("FLIGHT_NUM", StringType),
    StructField("ORIGIN", StringType),
    StructField("DEST", StringType),
    StructField("CRS_DEP_TIME", StringType),
    StructField("DEP_DELAY", FloatType),
    StructField("CRS_ARR_TIME", StringType),
    StructField("ARR_DELAY", FloatType),
    StructField("CANCELLED", FloatType),
    StructField("DIVERTED", FloatType)
  ))
}
