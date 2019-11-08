package com.lu.flink.table.tableapi

import com.lu.kafka.flink.utils.PropertiesUtils
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.table.api.{EnvironmentSettings, Over}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.scala._

object AggregationsExample {
  def main(args: Array[String]): Unit = {
    val properties = PropertiesUtils.getKafkaProperties()
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val consumer = new FlinkKafkaConsumer[String]("demo", new SimpleStringSchema(), properties)
    consumer.setStartFromEarliest()
    // 2019-11-06T17:59:27.592 100026,female,chengdu 958ed3e1-79d6-4208-b83d-7362dcea505f
    val stream = environment.addSource(consumer)
    val source = stream
      .map(line => {
        val strings = line.split(" ")
        val strings1 = strings(1).split(",")
        (strings1(1), 1)
      })

    val bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(environment, bsSettings)

    tableEnv.registerDataStream("test", source, 'gender, 'number, 'UserActionTime.rowtime)
    val test = tableEnv.scan("test")
    val result = test
      .window(Over
        partitionBy 'gender
        orderBy 'UserActionTime
        preceding UNBOUNDED_RANGE
        following CURRENT_RANGE
        as 'w)
      .select('gender, 'number.avg over 'w, 'number.max over 'w, 'number.min over 'w)
    val out = tableEnv.toAppendStream[(String, Int, Int, Int)](result)
    out.print()

    environment.execute()
  }

}
