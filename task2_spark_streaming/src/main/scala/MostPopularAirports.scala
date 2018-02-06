package capstone

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.streaming._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{StreamingContext,Seconds,StateSpec,State}

import cassandra.CassandraDriver

object G1Q1 {
  def main(args: Array[String]) {
    val inputPath = args(0)

    val sparkConf = new SparkConf().setAppName("most popular airports")
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    ssc.checkpoint("./.tmp/")

    initTable(sparkConf)
    val mappingFunc = (word: String, one: Option[Int], state: State[Int]) => {
      val sum = one.getOrElse(0) + state.getOption.getOrElse(0)
      val output = (word, sum)
      state.update(sum)
      output
    }

    val query = ssc
      .textFileStream(inputPath)
      .map(_.split('|'))
      .flatMap(fields => Array((fields(3),1), (fields(4),1)))
      .mapWithState(StateSpec.function(mappingFunc))
      .saveToCassandra("capstone", "airport_count")

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
        DROP TABLE IF EXISTS capstone.airport_count
      """)
      session.execute(s"""
        CREATE TABLE capstone.airport_count (key TEXT PRIMARY KEY, value BIGINT)
      """)
    }
  }
}
