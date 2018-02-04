import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object DataCleaner {
  def main(args: Array[String])  {
    val conf = new SparkConf().setAppName("data cleaner")
    val sc = new SparkContext(conf)
  }
}
