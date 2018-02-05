package cassandra

import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.Row

class CassandraForeachSink() extends ForeachWriter[Row] {
  private def processRecord(row: Row): String = s"""
       insert into capstone.test (key, value)
       values('hello world', 42)"""
       // values('${row.getString(0)}', '${row.getInt(1)}')"""

  override def open(partitionId: Long, version: Long) = true

  override def process(row: Row) = {
    CassandraDriver.connector.withSessionDo(session =>
      session.execute(processRecord(row))
    )
  }

  override def close(errorOrNull: Throwable) = {}
}
