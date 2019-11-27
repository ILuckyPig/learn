package com.lu.flink.streaming.operators.windows

import java.time.{LocalDateTime, ZoneOffset}

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

object AllowedLatenessExample {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val source = environment.socketTextStream("127.0.0.1", 9999)
    // 1,1,2019-11-25T18:58:06
    // 1,1,2019-11-25T18:58:00
    // 1,1,2019-11-25T18:58:07
    // 1,1,2019-11-25T18:58:11

    val late = OutputTag[(Int, Int, LocalDateTime)]("late-data")

    source
      .assignTimestampsAndWatermarks(new MyTimestampAndWatermarkGenerator)
      .map(message => {
        val strings = message.split(",")
        (strings(0).toInt, strings(1).toInt, LocalDateTime.parse(strings(2)))
      })
      .keyBy(0)
      .timeWindow(Time.seconds(10))
      .allowedLateness(Time.seconds(3))
      .sideOutputLateData(late)
      .aggregate(new MyAggregate)
      .print()

    // FIXME no print
    source
      .getSideOutput(late)
      .map(message => ("late", message))
      .print()

    environment.execute()
  }

  class MyTimestampAndWatermarkGenerator extends AssignerWithPeriodicWatermarks[String] {
    val maxTimeLong = 3000
    var currentMaxTimestamp: Long = _

    override def getCurrentWatermark: Watermark = {
      new Watermark(currentMaxTimestamp - maxTimeLong)
    }

    override def extractTimestamp(element: String, previousElementTimestamp: Long): Long = {
      val timestamp = LocalDateTime.parse(element.split(",")(2)).toInstant(ZoneOffset.of("+8")).toEpochMilli
      currentMaxTimestamp = Math.max(currentMaxTimestamp, timestamp)
      println("message: " + element + ",eventtime:" + timestamp + ",currentMaxTimestamp:" + currentMaxTimestamp+",watermark:" + getCurrentWatermark)
      timestamp
    }
  }

  class MyAggregate extends AggregateFunction[(Int, Int, LocalDateTime), (Int, Int, LocalDateTime), (Int, Int, LocalDateTime)] {
    override def createAccumulator(): (Int, Int, LocalDateTime) = (0, 0, LocalDateTime.parse("1970-01-01T00:00:00"))

    override def add(value: (Int, Int, LocalDateTime), accumulator: (Int, Int, LocalDateTime)): (Int, Int, LocalDateTime) = {
      (value._1, accumulator._2 + value._2, if (value._3.isAfter(accumulator._3)) value._3 else accumulator._3)
    }

    override def getResult(accumulator: (Int, Int, LocalDateTime)): (Int, Int, LocalDateTime) = accumulator

    override def merge(a: (Int, Int, LocalDateTime), b: (Int, Int, LocalDateTime)): (Int, Int, LocalDateTime) = {
      (a._1, a._2 + b._2, if (a._3.isAfter(b._3)) a._3 else b._3)
    }
  }
}