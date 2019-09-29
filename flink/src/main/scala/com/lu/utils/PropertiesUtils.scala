package com.lu.utils

import java.util.Properties

object PropertiesUtils {
  def getKafkaProperties(): Properties = {
    val properties = new Properties()
    properties.put("bootstrap.servers", "127.0.0.1:9092")
    properties.put("group.id", "demo-consumer")
    properties
  }
}
