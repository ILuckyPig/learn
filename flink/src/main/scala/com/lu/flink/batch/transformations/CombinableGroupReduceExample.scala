package lu.flink.batch.transformations

import java.lang

import org.apache.flink.api.common.functions.{GroupCombineFunction, GroupReduceFunction}
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

object CombinableGroupReduceExample {
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val source: DataSet[(String, Int)] = environment.fromElements(("a", 1), ("b", 1), ("a", 2), ("a", 2), ("b", 1))
    source.groupBy(0).combineGroup(new MyCombinableGroupReducer).print()

    // TODO 在IN OUT类型相同时，可以用combineGroup代替reduceGroup
  }

  class MyCombinableGroupReducer extends GroupReduceFunction[(String, Int), String]
    with GroupCombineFunction[(String, Int), (String, Int)] {
    override def reduce(iterable: lang.Iterable[(String, Int)], collector: Collector[String]): Unit = {
      val r: (String, Int) = iterable.iterator().asScala.reduce((a,b) => (a._1, a._2 + b._2))
      collector.collect(r._1 + " - " + r._2)
    }

    override def combine(iterable: lang.Iterable[(String, Int)], collector: Collector[(String, Int)]): Unit = {
      val r: (String, Int) = iterable.iterator().asScala.reduce((a,b) => (a._1, a._2 + b._2))
      collector.collect(r)
    }
  }
}
