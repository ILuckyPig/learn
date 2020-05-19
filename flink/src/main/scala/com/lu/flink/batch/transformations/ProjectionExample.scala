package lu.flink.batch.transformations

import org.apache.flink.api.java.DataSet
import org.apache.flink.api.java.tuple.Tuple3
import org.apache.flink.api.scala._

object ProjectionExample {
  def main(args: Array[String]): Unit = {
    val environment: org.apache.flink.api.java.ExecutionEnvironment = org.apache.flink.api.java.ExecutionEnvironment.getExecutionEnvironment
    val value = new Tuple3[Integer, Integer, Integer](1, 2, 3)
    val source: DataSet[Tuple3[Integer, Integer, Integer]] = environment.fromElements(value)
    source.project(0, 2).print()
  }
}
