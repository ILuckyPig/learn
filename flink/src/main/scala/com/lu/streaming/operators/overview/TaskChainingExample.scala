package com.lu.streaming.operators.overview

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object TaskChainingExample {
  def main(args: Array[String]): Unit = {
    // Use StreamExecutionEnvironment.disableOperatorChaining() if you want to disable chaining in the whole job
    val environment = StreamExecutionEnvironment.getExecutionEnvironment.disableOperatorChaining

    val dataStream = environment.fromElements((1, 1), (1, 2), (1, 3), (2, 1), (3, 1), (2, 2))

    // The two mappers will be chained, and filter will not be chained to the first mapper.
    dataStream
      .filter(_._1 % 2 == 0)
      .map(x => (x._1, x._2 + 1))
      .startNewChain()
      .map(x => (x._1, x._2 + 1))
      .print()

    // Do not chain the map operator
    dataStream
      .filter(_._1 % 2 == 0)
      .map(x => (x._1, x._2 + 1))
      .disableChaining()
      .map(x => (x._1, x._2 + 1))
      .print()

    // Set the slot sharing group of an operation.
    // Flink will put operations with the same slot sharing group into the same slot
    // while keeping operations that don't have the slot sharing group in other slots.
    dataStream
      .filter(_._1 % 2 == 0)
      // 明确该operation会投放到名为slot_sharing_group_name的slots中
      .slotSharingGroup("slot_sharing_group_name")
      // 默认情况下是default
      .slotSharingGroup("default")
      .print()


    environment.execute()
  }
}
