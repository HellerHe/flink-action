package com.czb

import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleDeserializers
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}

import java.util.Properties

object KeyedState {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "bg1:9092")
    properties.setProperty("group.id", "con")
    env.addSource(new FlinkKafkaConsumer[String]("con", new SimpleStringSchema(), properties))
      .map(new LineToLog)

    env.execute()
  }

  class LineToLog extends RichMapFunction[String, Log]{
    var value: ValueState[String] = _

    override def open(parameters: Configuration): Unit = {
      value = getRuntimeContext.getState[String](new ValueStateDescriptor[String]("user_id", classOf[String]))
    }

    override def map(in: String): Log = {
      val arr = in.split(",")
      value.update(arr(0))
      Log(arr(0).toInt, arr(1).toInt, arr(3).toInt, arr(2).toLong)
    }
  }
}
