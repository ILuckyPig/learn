package com.lu.flink.streaming.overview

import com.lu.kafka.flink.utils.PropertiesUtils
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object OverviewDemo {
  def main(args: Array[String]): Unit = {
    val properties = PropertiesUtils.getKafkaProperties()
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = environment.addSource(new FlinkKafkaConsumer[String]("demo", new SimpleStringSchema(), properties))
    stream
      .map(message => {
        val strings = message.split("""\s""")
        val dateTime = strings(0)
        val user = strings(1)
        val context = strings(2)
        val city = user.split(",")(2)
        (city, 1)
      })
      .keyBy(0)
      .timeWindow(Time.seconds(10))
      .sum(1)
      .print()
    environment.execute()
  }
}
