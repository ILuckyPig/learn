package com.lu.flink.streaming.operators.windows

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time

object SessionWindowExample {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    val source = environment.socketTextStream("localhost", 9999, '\n')

    // processing-time session windows with static gap
    source
      .map(line => (line.split(",")(0).toInt, line.split(",")(1).toInt))
      .keyBy(0)
      .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
      .sum(1)
      .print()



    environment.execute()
  }
}
