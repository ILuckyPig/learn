package com.lu.flink.streaming.operators.joining

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{ProcessingTimeSessionWindows, SlidingProcessingTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

object WindowJoinExample {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    val firstSource = environment.socketTextStream("127.0.0.1", 9999).map(message => (message.toInt, 1))
    val secondSource = environment.socketTextStream("127.0.0.1", 9998).map(message => (message.toInt, 2))

    // TumblingTime Window
    firstSource
      .join(secondSource)
      .where(_._1)
      .equalTo(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .apply((e1, e2) => (e1._1, e1._2 + e2._2))
      .print()

    // SlidingTime Window
    firstSource
      .join(secondSource)
      .where(_._1)
      .equalTo(_._1)
      .window(SlidingProcessingTimeWindows.of(Time.seconds(3), Time.seconds(1)))
      .apply((e1, e2) => (e1._1, e1._2 + e2._2))
      .print()

    // SessionTime Window
    firstSource
      .join(secondSource)
      .where(_._1)
      .equalTo(_._1)
      .window(ProcessingTimeSessionWindows.withGap(Time.seconds(3)))
      .apply((e1, e2) => (e1._1, e1._2 + e2._2))
      .print()

    environment.execute()
  }
}
