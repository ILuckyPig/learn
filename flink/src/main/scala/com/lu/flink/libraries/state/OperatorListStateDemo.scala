package com.lu.flink.libraries.state

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.state.api.Savepoint

object OperatorListStateDemo {
  def main(args: Array[String]): Unit = {
    val environment = ExecutionEnvironment.getExecutionEnvironment
    val savepoint = Savepoint.load(environment, "file:///", new MemoryStateBackend())
  }
}
