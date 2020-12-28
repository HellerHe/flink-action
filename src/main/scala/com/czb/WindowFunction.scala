package com.czb

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.util.Properties

object WindowFunction {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)
    env.getConfig.setAutoWatermarkInterval(500)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "bg1:9092")
    properties.setProperty("group.id", "con")
    val value = OutputTag.apply[Log]("late")

    env.addSource(new FlinkKafkaConsumer[String]("con", new SimpleStringSchema(), properties))
      .map(line => {
        val arr = line.split(",")
        Log(arr(0).toInt, arr(1).toInt, arr(3).toInt, arr(2).toLong)
      }
      )
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Log](Time.milliseconds(500)) {
        override def extractTimestamp(t: Log): Long = t.timestamp
      })
      .keyBy(_.userId)
      .window(TumblingEventTimeWindows.of(Time.minutes(1)))
      .allowedLateness(Time.milliseconds(500))
      .sideOutputLateData(value)
      .reduce((a, b) => b)
      .print().setParallelism(1)


    env.execute()
  }

}
