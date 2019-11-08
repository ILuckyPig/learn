package com.lu.flink.streaming.operators.windows

import com.lu.kafka.flink.utils.PropertiesUtils
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

object IncrementalWindowFunctionExample {
  def main(args: Array[String]): Unit = {
    val properties = PropertiesUtils.getKafkaProperties()
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    val consumer = new FlinkKafkaConsumer[String]("demo", new SimpleStringSchema(), properties)
    consumer.setStartFromEarliest()
    val stream = environment.addSource(consumer).map(message => (message.split(" ")(1).split(",")(1), message.split(" ")(0)))

    stream
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      // reduce with incremental
      .reduce(
        (r1: (String, String), r2: (String, String)) => { if (r1._2 > r2._2) r2 else r1 },
        new MyProcessWindowFunction
      )
      .print()

    environment.execute()
  }

  // 返回窗口的开始时间和时间最小的元素
  class MyProcessWindowFunction extends ProcessWindowFunction[(String, String), (Long, String), Tuple, TimeWindow] {
    override def process(key: Tuple,
                         context: Context,
                         elements: Iterable[(String, String)],
                         out: Collector[(Long, String)]): Unit = {
      val min = elements.iterator.next()
      out.collect((context.window.getStart, min._2))
    }
  }
}
