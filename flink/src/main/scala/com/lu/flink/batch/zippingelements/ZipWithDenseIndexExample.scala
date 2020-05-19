package lu.flink.batch.zippingelements

import org.apache.flink.api.java.utils.DataSetUtils
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

object ZipWithDenseIndexExample {
  def main(args: Array[String]): Unit = {
    val environment = ExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(2)
    val input: DataSet[String] = environment.fromElements("A", "B", "C", "D", "E", "F", "G", "H")
    //val result: DataSet[(Long, String)] = DataSetUtils.zipWithIndex(input)
  }
}
