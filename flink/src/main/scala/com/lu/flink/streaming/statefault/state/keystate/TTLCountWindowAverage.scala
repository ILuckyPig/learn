package com.lu.flink.streaming.statefault.state.keystate

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

class TTLCountWindowAverage(ttlConfig: StateTtlConfig) extends RichFlatMapFunction[(Long, Long), (Long, Long)] {
  private var sum: ValueState[(Long, Long)] = _

  override def flatMap(value: (Long, Long), out: Collector[(Long, Long)]): Unit = {
    val tmpCurrentSum = sum.value
    val currentSum = if (null != tmpCurrentSum) {
      tmpCurrentSum
    } else {
      (0L, 0L)
    }

    val newSum = (currentSum._1 + 1, currentSum._2 + value._2)
    sum.update(newSum)
    if (newSum._1 >= 2) {
      out.collect((value._1, newSum._2 / newSum._1))
      sum.clear()
    }
  }

  override def open(parameters: Configuration): Unit = {
    val valueStateDescriptor = new ValueStateDescriptor[(Long, Long)]("average", classOf[(Long, Long)])
    valueStateDescriptor.enableTimeToLive(ttlConfig)
    sum = getRuntimeContext.getState(valueStateDescriptor)
  }
}
