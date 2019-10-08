package com.lu.streaming.statefault.checkpointing

import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object CheckpointingExample {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    // 每1000ms开始一个checkpoint
    environment.enableCheckpointing(1000)

    // checkpoint高级设置：
    // set mode to exactly-once (default)
    environment.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    // checkpoint间最小时间间隔（无视checkpoint创建时间以及间隔时间），也就是说checkpoint间隔绝不会小于这个时间
    environment.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)

    // checkpoint超时时间，checkpoint必须在该时间内完成，否则被丢弃
    environment.getCheckpointConfig.setCheckpointTimeout(60000)

    // checkpoint失败机制
    // setFailOnCheckpointingErrors(true) = setTolerableCheckpointFailureNumber(0) -> checkpoint失败，整个job失败
    // setFailOnCheckpointingErrors(false) = setTolerableCheckpointFailureNumber(n) -> checkpoint失败不会影响job
    environment.getCheckpointConfig.setTolerableCheckpointFailureNumber(1);

    // 默认情况下在进行一个checkpoint时不会进行另一个checkpoint
    // 可以同时进行的checkpoint数量
    environment.getCheckpointConfig.setMaxConcurrentCheckpoints(1);

    // checkpoint会从最新的checkpoint恢复，即使有距离现在更近的savepoint
    environment.getCheckpointConfig.setPreferCheckpointForRecovery(true)
  }
}
