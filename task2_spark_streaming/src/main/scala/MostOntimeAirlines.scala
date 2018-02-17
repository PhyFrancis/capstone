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

case class OntimeSummary(count: Int, total_delay: Double)

object G1Q2 {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("most ontime airlines")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(10))
    ssc.checkpoint("hdfs://ip-172-31-5-186:9000/G1Q2_tmp")
    
    val fs = FileSystem.get(new URI("hdfs://ip-172-31-5-186:9000"), sc.hadoopConfiguration);
    if(fs.exists(new Path("/G1Q2_tmp"))) {
      fs.delete(new Path("/G1Q2_tmp"),true)
    }

    val topicsSet = Set("cleaned_data")
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "172.31.5.186:9092")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    initTable(sparkConf)
    val updateFunction = 
      (delays: Seq[Double], summary:Option[OntimeSummary]) => {
        val state = summary.getOrElse(OntimeSummary(0, 0.0))
        var count = state.count;
        var delay = state.total_delay;
        for (d <- delays) {
          delay += d;
          count += 1;
        }
        Some(OntimeSummary(count, delay));
      }

    val query = messages
      .map(m => m._2.split('|'))
      .filter(fields => fields(8).length > 0 && fields(9).toDouble == 0.0 && fields(10).toDouble == 0.0)
      .map(fields => (fields(1), fields(8).toDouble))
      .updateStateByKey[OntimeSummary](updateFunction)
      .map(s => (s._1, s._2.total_delay / s._2.count))
      .saveToCassandra("capstone", "airline_ontime_arrival")

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
        DROP TABLE IF EXISTS capstone.airline_ontime_arrival
      """)
      session.execute(s"""
        CREATE TABLE capstone.airline_ontime_arrival (airline TEXT, arrival_delay DOUBLE, PRIMARY KEY (airline))
      """)
    }
  }
}
