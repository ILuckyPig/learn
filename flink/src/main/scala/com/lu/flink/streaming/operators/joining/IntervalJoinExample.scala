package com.lu.flink.streaming.operators.joining

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object IntervalJoinExample {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val firstSource = environment.socketTextStream("127.0.0.1", 9999)
      .map(message => (message.toInt, 1))
      .assignTimestampsAndWatermarks(new TimeLagWatermarkGenerator)
    val secondSource = environment.socketTextStream("127.0.0.1", 9998)
      .map(message => (message.toInt, 2))
      .assignTimestampsAndWatermarks(new TimeLagWatermarkGenerator)

    firstSource
      .keyBy(_._1)
      .intervalJoin(secondSource.keyBy(_._1))
      .between(Time.seconds(-2), Time.seconds(1))
      .process(new ProcessJoinFunction[(Int, Int), (Int, Int), String] {
        override def processElement(left: (Int, Int),
                                    right: (Int, Int),
                                    ctx: ProcessJoinFunction[(Int, Int), (Int, Int), String]#Context,
                                    out: Collector[String]): Unit = {
          out.collect(left + ", " + right)
        }
      })
      .print()
    environment.execute()
  }

  class TimeLagWatermarkGenerator extends AssignerWithPeriodicWatermarks[(Int, Int)] {

    override def extractTimestamp(element: (Int, Int), previousElementTimestamp: Long): Long = {
      System.currentTimeMillis()
    }

    override def getCurrentWatermark: Watermark = {
      // return the watermark as current time minus the maximum time lag
      new Watermark(System.currentTimeMillis())
    }
  }
}
