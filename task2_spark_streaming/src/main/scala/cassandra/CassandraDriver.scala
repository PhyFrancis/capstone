package cassandra

import org.apache.spark.sql.SparkSession
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector

object CassandraDriver {
  val connector = CassandraConnector(SparkSession.builder().getOrCreate().sparkContext.getConf)

  // def saveForeach(df: DataFrame ) = {
  //   val ds = CassandraDriver.getDatasetForCassandra(df)

  //   ds
  //     .writeStream
  //     .queryName("KafkaToCassandraForeach")
  //     .outputMode("update")
  //     .foreach(new CassandraSinkForeach())
  //     .start()
  // }
}
