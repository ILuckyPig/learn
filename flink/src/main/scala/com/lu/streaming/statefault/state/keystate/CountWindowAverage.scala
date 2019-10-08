package com.lu.streaming.statefault.state.keystate

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

class CountWindowAverage extends RichFlatMapFunction[(Long, Long), (Long, Long)] {
  private var sum: ValueState[(Long, Long)] = _

  override def flatMap(in: (Long, Long), collector: Collector[(Long, Long)]): Unit = {
    // get the state value
    val tmpCurrentSum = sum.value

    val currentSum = if (null != tmpCurrentSum) {
      tmpCurrentSum
    } else {
      (0L, 0L)
    }

    // update count and sum
    val newSum = (currentSum._1 + 1, currentSum._2 + in._2)

    // update the state
    sum.update(newSum)

    // if the count reaches 2, emit the average and clear the state
    if (newSum._1 >= 2) {
      collector.collect((in._1, newSum._2 / newSum._1))
      sum.clear()
    }
  }

  override def open(parameters: Configuration): Unit = {
    sum = getRuntimeContext.getState(new ValueStateDescriptor[(Long, Long)]("average", classOf[(Long, Long)]))
  }
}
