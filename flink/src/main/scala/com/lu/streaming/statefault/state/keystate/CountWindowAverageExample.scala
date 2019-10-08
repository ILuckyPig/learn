package com.lu.streaming.statefault.state.keystate

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object CountWindowAverageExample {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    val source = environment.socketTextStream("localhost", 9999, '\n')
      source
      .map(line => (line.split(",")(0).toLong, line.split(",")(1).toLong))
      .keyBy(0)
      .flatMap(new CountWindowAverage())
      .print()
    environment.execute()
  }
}
