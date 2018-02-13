package capstone

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.streaming._
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{StreamingContext,Seconds,StateSpec,State}
import org.apache.spark.streaming.kafka._

case class OntimeSummary(count: Int, total_delay: Double)

object G1Q2 {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("most ontime airlines")
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    ssc.checkpoint("hdfs://ip-172-31-5-186:9000/G1Q2_tmp")

    val topicsSet = Set("cleaned_data")
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "172.31.5.186:9092")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    initTable(sparkConf)
    val mappingFunc = (airline: String, delay: Option[Double], state: State[OntimeSummary]) => {
      val old_state = state.getOption.getOrElse(OntimeSummary(0, 0.0))
      val new_state = OntimeSummary(old_state.count + 1, old_state.total_delay + delay.getOrElse(0.0))
      state.update(new_state)
      (airline, new_state.total_delay / new_state.count)
    }

    val query = messages
      .map(m => m._2.split('|'))
      .filter(fields => fields(8).length > 0 && fields(9).toDouble == 0.0 && fields(10).toDouble == 0.0)
      .map(fields => (fields(1),fields(8).toDouble))
      .mapWithState(StateSpec.function(mappingFunc))
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
        CREATE TABLE capstone.airline_ontime_arrival (airline TEXT PRIMARY KEY, arrival_delay DOUBLE)
      """)
    }
  }
}
