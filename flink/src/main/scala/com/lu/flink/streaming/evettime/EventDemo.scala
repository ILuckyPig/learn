package com.lu.flink.streaming.evettime

import java.util

import com.lu.kafka.flink.utils.PropertiesUtils
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

object EventDemo {
  def main(args: Array[String]): Unit = {
    val properties = PropertiesUtils.getKafkaProperties()
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val consumer = new FlinkKafkaConsumer[String]("demo", new SimpleStringSchema(), properties)
//    consumer.setStartFromEarliest()
    val stream = environment.addSource(consumer)
    val withTimestampsAndWatermarks = stream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator)
    withTimestampsAndWatermarks
      .timeWindowAll(Time.seconds(5))
      .apply((window, input, out: Collector[String]) => {
        val result = new util.ArrayList[String]()
        input
          .foreach(s => {
            result.add(s)
            println("message: " + s + ",eventtime:" + s + ",window_start:" + window.getStart +",window_end:" + window.getEnd)
          })
        val message = String.join(",", result)
        out.collect(message)
      })
      .print()
    environment.execute()
  }
}
