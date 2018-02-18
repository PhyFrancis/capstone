package capstone

import org.apache.spark.sql.types._

object Common {
  def schema = StructType(Array(
    /*0 */ StructField("FLIGHT_DATE", DateType),
    /*1 */ StructField("UNIQUE_CARRIER", StringType),
    /*2 */ StructField("FLIGHT_NUM", StringType),
    /*3 */ StructField("ORIGIN", StringType),
    /*4 */ StructField("DEST", StringType),
    /*5 */ StructField("CRS_DEP_TIME", StringType),
    /*6 */ StructField("DEP_DELAY", FloatType),
    /*7 */ StructField("CRS_ARR_TIME", StringType),
    /*8 */ StructField("ARR_DELAY", FloatType),
    /*9 */ StructField("CANCELLED", FloatType),
    /*10*/ StructField("DIVERTED", FloatType)
  ))
}

case class OntimeSummary(count: Int, total_delay: Double)

case class AirportCarrier(airport: String, carrier: String)

case class AirportAirport(origin: String, dest: String)
