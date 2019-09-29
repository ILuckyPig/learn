package com.lu.streaming.evettime

import com.lu.utils.PropertiesUtils
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object EventDemo {
  def main(args: Array[String]): Unit = {
    val properties = PropertiesUtils.getKafkaProperties()
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream = environment.addSource(new FlinkKafkaConsumer[String]("demo", new SimpleStringSchema(), properties))
    val withTimestampsAndWatermarks = stream.assignTimestampsAndWatermarks(new TimeLagWatermarkGenerator)
    withTimestampsAndWatermarks
      .timeWindowAll(Time.seconds(5))
      .reduce((v1, v2) => v1 + ","+ v2)
      .print()
    environment.execute()
  }
}
