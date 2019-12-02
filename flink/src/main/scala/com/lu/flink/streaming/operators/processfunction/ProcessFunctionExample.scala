package lu.flink.streaming.operators.processfunction

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object ProcessFunctionExample {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    environment.getConfig.setAutoWatermarkInterval(1000)
    val source = environment.socketTextStream("127.0.0.1", 9999).map(message => (message.toInt, 1))
    source
      .keyBy(0)
      .process(new CountWithTimeoutFunction())
      .print()
    environment.execute()
  }

  /**
   * The data type stored in the state
   */
  case class CountWithTimestamp(key: Int, count: Int, lastModified: Long)

  /**
   * The implementation of the ProcessFunction that maintains the count and timeouts
   */
  class CountWithTimeoutFunction extends KeyedProcessFunction[Tuple, (Int, Int), (Int, Long)] {

    /** The state that is maintained by this process function */
    lazy val state: ValueState[CountWithTimestamp] = getRuntimeContext
      .getState(new ValueStateDescriptor[CountWithTimestamp]("myState", classOf[CountWithTimestamp]))


    override def processElement(value: (Int, Int),
                                ctx: KeyedProcessFunction[Tuple, (Int, Int), (Int, Long)]#Context,
                                out: Collector[(Int, Long)]): Unit = {

      // initialize or retrieve/update the state
      val current: CountWithTimestamp = state.value match {
        case null =>
          println("叫裂空", ctx)
          CountWithTimestamp(value._1, 1, ctx.timestamp)
        case CountWithTimestamp(key, count, lastModified) =>
          CountWithTimestamp(key, count + 1, ctx.timestamp)
      }

      // write the state back
      state.update(current)

      // schedule the next timer 60 seconds from the current event time
      ctx.timerService.registerEventTimeTimer(current.lastModified + 60000)
    }

    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[Tuple, (Int, Int), (Int, Long)]#OnTimerContext,
                         out: Collector[(Int, Long)]): Unit = {

      state.value match {
        case CountWithTimestamp(key, count, lastModified) if (timestamp == lastModified + 60000) =>
          out.collect((key, count))
        case _ =>
      }
    }
  }
}
