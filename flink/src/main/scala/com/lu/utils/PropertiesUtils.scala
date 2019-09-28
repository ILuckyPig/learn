package com.lu.utils

import java.util.Properties

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object PropertiesUtils {
  def getKafkaProperties(): Properties = {
    val properties = new Properties()
    properties.put("bootstrap.servers", "192.168.1.5:9092")
    properties.put("group.id", "demo-consumer")
    properties
  }
}
