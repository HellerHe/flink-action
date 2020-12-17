package com.czb

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.sql.{PreparedStatement, Timestamp}
import java.util.{Calendar, Locale, Properties, TimeZone}

object LogSinkMySQL {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "bg1:9092")
    properties.setProperty("group.id", "con")

    env.addSource(new FlinkKafkaConsumer[String]("con", new SimpleStringSchema(), properties))
      .map(line => {
        val arr = line.split(",")
        (arr(0), arr(1), arr(2), arr(3))
      }
      )
      .addSink(JdbcSink.sink("insert into log (user_id, product_id, behavior, timestamp) values ( ?, ?, ?, ?)",
        new MysqlSink,
        JdbcExecutionOptions.builder()
          .withBatchSize(10)
          .build(),
        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
          .withUrl("jdbc:mysql://bg1:3306/recommand")
          .withDriverName("com.mysql.cj.jdbc.Driver")
          .withUsername("root")
          .withPassword("heller")
          .build()
      ))
    env.execute()
  }

  class MysqlSink extends JdbcStatementBuilder[(String, String, String, String)] {
    override def accept(t: PreparedStatement, u: (String, String, String, String)): Unit = {
      t.setInt(1, u._1.toInt)
      t.setInt(2, u._2.toInt)
      t.setInt(3, u._4.toInt)
      t.setTimestamp(4, new Timestamp(u._3.toLong), Calendar.getInstance(Locale.CHINESE))
    }
  }

}
