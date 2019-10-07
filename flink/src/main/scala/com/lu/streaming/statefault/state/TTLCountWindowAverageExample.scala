package com.lu.streaming.statefault.state

import org.apache.flink.api.common.state.StateTtlConfig
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object TTLCountWindowAverageExample {
  def main(args: Array[String]): Unit = {
    val ttlConfig = StateTtlConfig
      .newBuilder(Time.seconds(1))
      .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
      .build
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    val source = environment.socketTextStream("localhost", 9999, '\n')
    source
      .map(line => (line.split(",")(0).toLong, line.split(",")(1).toLong))
      .keyBy(0)
      .flatMap(new TTLCountWindowAverage(ttlConfig))
      .print()
    environment.execute()
  }
}
