package com.lu.flink.streaming.operators.asyncio

import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.async.ResultFuture

import scala.concurrent.{ExecutionContext, Future}

object AsyncIOExample {
  def main(args: Array[String]): Unit = {
    val timeout = 10000L
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val input = env.addSource(new SimpleSource())

    val asyncMapped = AsyncDataStream.orderedWait(input, timeout, TimeUnit.MILLISECONDS, 10) {
      (input, collector: ResultFuture[Int]) =>
        Future {
          collector.complete(Seq(input))
        } (ExecutionContext.global)
    }

    asyncMapped.print()

    env.execute("Async I/O job")
  }

  class SimpleSource extends ParallelSourceFunction[Int] {
    var running = true
    var counter = 0

    override def run(ctx: SourceContext[Int]): Unit = {
      while (running) {
        ctx.getCheckpointLock.synchronized {
          ctx.collect(counter)
        }
        counter += 1
        Thread.sleep(10L)
      }
    }

    override def cancel(): Unit = {
      running = false
    }
  }
}
