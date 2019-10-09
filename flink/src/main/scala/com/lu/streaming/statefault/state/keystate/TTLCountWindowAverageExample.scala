package com.lu.streaming.statefault.state.keystate

import org.apache.flink.api.common.state.StateTtlConfig
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/**
 * 状态有效期
 * 下例：
 * state会保留10秒的状态，输入(1,4) 10s内，再输入(1,6)，输出(1,5)；
 * 超过10s则state被清除，不会输出(1,5)
 */
object TTLCountWindowAverageExample {
  def main(args: Array[String]): Unit = {
    val ttlConfig = StateTtlConfig
      .newBuilder(Time.seconds(10))
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
