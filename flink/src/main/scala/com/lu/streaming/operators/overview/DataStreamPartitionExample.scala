package com.lu.streaming.operators.overview

import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object DataStreamPartitionExample {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    val dataStream = environment.fromElements((1,1), (1,2), (1,3), (2,1), (3,1), (2,2))

    // 1. partitionCustom - uses a user-defined Partitioner to select the target for each element
    val partitioner = new MyPartitioner
    val partitionStream = dataStream.partitionCustom(partitioner, 0)
    partitionStream.print()
    /**
     * 这个dataStream按照key的奇偶分成了两个分区
     * 1> (2,1)
     * 2> (1,1)
     * 2> (1,2)
     * 1> (2,2)
     * 2> (1,3)
     * 2> (3,1)
     */

    // 2. shuffle - 均匀随机地进行分区
    partitionStream.shuffle.print()

    // 3. rebalance - 循环地分区元素，可用于数据倾斜的情况下
    partitionStream.rebalance.print()
    /**
     * 5> (1,2)
     * 6> (1,3)
     * 8> (3,1)
     * 7> (2,1)
     * 1> (2,2)
     * 4> (1,1)
     */
    // 4. rescale - Partitions elements, round-robin, to a subset of downstream operations
    partitionStream.rescale.print()

    // 5. broadcast - 广播元素到每一个分区
    partitionStream.broadcast.print()
    /**
     * 7> (1,1)
     * 6> (1,1)
     * 1> (1,1)
     * 4> (1,1)
     * 2> (1,1)
     * 3> (1,1)
     * 6> (1,2)
     * 5> (1,1)
     * 5> (1,2)
     * 1> (1,2)
     * 8> (1,1)
     * 1> (1,3)
     * 5> (1,3)
     * 1> (2,1)
     * 3> (1,2)
     * 6> (1,3)
     * 2> (1,2)
     * 4> (1,2)
     * 7> (1,2)
     * 4> (1,3)
     * 2> (1,3)
     * 6> (2,1)
     * 3> (1,3)
     * 1> (3,1)
     * 5> (2,1)
     * 8> (1,2)
     * 5> (3,1)
     * 1> (2,2)
     * 3> (2,1)
     * 6> (3,1)
     * 2> (2,1)
     * 4> (2,1)
     * 7> (1,3)
     * 4> (3,1)
     * 3> (3,1)
     * 2> (3,1)
     * 3> (2,2)
     * 6> (2,2)
     * 5> (2,2)
     * 8> (1,3)
     * 2> (2,2)
     * 4> (2,2)
     * 7> (2,1)
     * 8> (2,1)
     * 8> (3,1)
     * 8> (2,2)
     * 7> (3,1)
     * 7> (2,2)
     */

    environment.execute()
  }

  /**
   * 自定义分区
   */
  class MyPartitioner extends Partitioner[Int] {
    override def partition(key: Int, partitions: Int): Int = {
      if (key % 2 == 0) {
        0
      } else {
        1
      }
    }
  }
}
