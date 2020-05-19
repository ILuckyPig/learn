package lu.flink.batch.transformations

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

object MapPartitionExample {
  def main(args: Array[String]): Unit = {
    val environment = ExecutionEnvironment.getExecutionEnvironment
    val source = environment.fromElements(1, 2, 3, 4, 6)

    source.mapPartition((iter: Iterator[Int], out: Collector[Int]) => {
      var sum: Int = 0
      while (iter.hasNext) {
        sum = sum + iter.next()
      }
      out.collect(sum)
    }).print()
  }
}
