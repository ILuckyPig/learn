package com.lu.streaming.evettime

import com.lu.utils.PropertiesUtils
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object EventDemo {
  def main(args: Array[String]): Unit = {
    val properties = PropertiesUtils.getKafkaProperties()
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream = environment.addSource(new FlinkKafkaConsumer[String]("demo", new SimpleStringSchema(), properties))
    stream.print()
    environment.execute()
  }
}
