package capstone

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.streaming._
import kafka.serializer.StringDecoder
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{StreamingContext,Seconds,StateSpec,State}
import org.apache.spark.streaming.kafka._

import java.net.URI

// Find connecting flights for given route.
object G3Q2 {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("G3Q2")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(10))
    ssc.checkpoint("hdfs://ip-172-31-5-186:9000/G3Q2_tmp")
    
    val fs = FileSystem.get(new URI("hdfs://ip-172-31-5-186:9000"), sc.hadoopConfiguration);
    if(fs.exists(new Path("/G3Q2_tmp"))) {
      fs.delete(new Path("/G3Q2_tmp"),true)
    }

    val topicsSet = Set("cleaned_data")
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "172.31.5.186:9092")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    initTable(sparkConf)
    val updateFunction = 
      (delays: Seq[Double], delay:Option[Double]) => {
        Some((delays :+ delay.getOrElse(1000.0)).min)
      }

    val query = messages
      .map(m => m._2.split('|'))
      .filter(fields => fields(0).startsWith("2008") && fields(8).length > 0 && fields(9).toDouble == 0.0 && fields(10).toDouble == 0.0)
      .map(fields => (AirportAirportDate(fields(3), fields(4), fields(0)), fields(8).toDouble))
      .updateStateByKey[Double](updateFunction)
      .map(s => (s._1.origin, s._1.dest, s._1.date, s._2))
      .saveToCassandra("capstone", "origin_dest_date")

    ssc.start()
    ssc.awaitTermination()
  }

  def initTable(sparkConf: SparkConf) {
    CassandraConnector(sparkConf).withSessionDo { session =>
      session.execute(s"""
        CREATE KEYSPACE IF NOT EXISTS capstone
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
      """)
      session.execute(s"""
        DROP TABLE IF EXISTS capstone.origin_dest_date
      """)
      session.execute(s"""
        CREATE TABLE capstone.origin_dest_date (origin TEXT, dest TEXT, date DATE, arrival_delay DOUBLE, PRIMARY KEY (origin, dest, date))
      """)
    }
  }
}
