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

// For each source-destination pair X-Y, determine the mean arrival delay
// (in minutes) for a flight from X to Y.
object G2Q4 {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("G2Q4")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(10))
    ssc.checkpoint("hdfs://ip-172-31-5-186:9000/G2Q4_tmp")
    
    val fs = FileSystem.get(new URI("hdfs://ip-172-31-5-186:9000"), sc.hadoopConfiguration);
    if(fs.exists(new Path("/G2Q4_tmp"))) {
      fs.delete(new Path("/G2Q4_tmp"),true)
    }

    val topicsSet = Set("cleaned_data")
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "172.31.5.186:9092")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    initTable(sparkConf)
    val updateFunction = 
      (delays: Seq[Double], summary:Option[OntimeSummary]) => {
        val state = summary.getOrElse(OntimeSummary(0, 0.0))
        Some(OntimeSummary(state.count + delays.size, state.total_delay + delays.sum))
      }

    val query = messages
      .map(m => m._2.split('|'))
      .filter(fields => fields(8).length > 0 && fields(9).toDouble == 0.0 && fields(10).toDouble == 0.0)
      .map(fields => (AirportAirport(fields(3), fields(4)), fields(8).toDouble))
      .updateStateByKey[OntimeSummary](updateFunction)
      .map(s => (s._1.origin, s._1.dest, s._2.total_delay / s._2.count))
      .saveToCassandra("capstone", "origin_dest_ontime_arrival")

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
        DROP TABLE IF EXISTS capstone.origin_dest_ontime_arrival
      """)
      session.execute(s"""
        CREATE TABLE capstone.origin_dest_ontime_arrival (origin TEXT, dest TEXT, arrival_delay DOUBLE, PRIMARY KEY (origin, dest))
      """)
    }
  }
}
