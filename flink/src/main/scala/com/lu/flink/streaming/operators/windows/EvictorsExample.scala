package com.lu.flink.streaming.operators.windows

import java.lang

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.evictors.Evictor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue

object EvictorsExample {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    val source = environment.socketTextStream("127.0.0.1", 9999)
    source
        .map(message => message.toInt)
        .timeWindowAll(Time.seconds(10))
        .evictor(new MyEvictor)
        .sum(0)
        .print()
    environment.execute()
  }

  class MyEvictor extends Evictor[Int, TimeWindow] {
    override def evictBefore(elements: lang.Iterable[TimestampedValue[Int]],
                             size: Int,
                             window: TimeWindow,
                             evictorContext: Evictor.EvictorContext): Unit = {
      println(s"before size $size")
      println("before window start " + window.getStart)
      println("before window end " + window.getEnd)
      println("after processing time " + evictorContext.getCurrentProcessingTime)
    }

    override def evictAfter(elements: lang.Iterable[TimestampedValue[Int]],
                            size: Int,
                            window: TimeWindow,
                            evictorContext: Evictor.EvictorContext): Unit = {
      println(s"before size $size")
      println("before window start " + window.getStart)
      println("after window end " + window.getEnd)
      println("after processing time " + evictorContext.getCurrentProcessingTime)
    }
  }
}
