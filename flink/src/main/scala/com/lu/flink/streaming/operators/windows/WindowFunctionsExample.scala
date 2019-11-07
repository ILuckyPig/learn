package com.lu.flink.streaming.operators.windows

import com.lu.kafka.flink.utils.PropertiesUtils
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.{TimeWindow, Window}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

object WindowFunctionsExample {
  def main(args: Array[String]): Unit = {
    val properties = PropertiesUtils.getKafkaProperties()
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val consumer = new FlinkKafkaConsumer[String]("demo", new SimpleStringSchema(), properties)
    val stream = environment.addSource(consumer).map(line => (line.split(" ")(1).split(",")(1), 1))

    // reduce function
    stream
      .keyBy(0)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .reduce((v1, v2) => (v1._1, v1._2 + v2._2))
      .print()

    // fold function
    stream
      .keyBy(0)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      // deprecated use aggregate instead
      .fold("") { (acc, v) => acc + v._2 }

    // process window function
    // 更灵活的api，但是牺牲了性能和资源
    // 因为不能窗口内的元素不能被逐步聚合，而是会将数据缓冲直到窗口准备处理
    stream
      .keyBy(0)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .process(new MyProcessWindowFunction)
      .print()

    environment.execute()
  }

  // KEY must be org.apache.flink.api.java.tuple.Tuple
  class MyProcessWindowFunction extends ProcessWindowFunction[(String, Int), (String, Int), Tuple, TimeWindow] {
    override def process(key: Tuple,
                         context: Context,
                         elements: Iterable[(String, Int)],
                         out: Collector[(String, Int)]): Unit = {
      var count = 0
      for (element <- elements) {
        count = count + element._2
      }
      out.collect((key.getField[String](0), count))
    }
  }
}
