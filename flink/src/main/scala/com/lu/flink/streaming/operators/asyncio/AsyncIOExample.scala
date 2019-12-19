package lu.flink.streaming.operators.asyncio

import java.util

import org.apache.flink.runtime.concurrent.Executors
import org.apache.flink.streaming.api.functions.async.{AsyncFunction, ResultFuture}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

import scala.concurrent.{ExecutionContext, Future}

object AsyncIOExample {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    val source = environment.socketTextStream("127.0.0.1", 9999).map(_.toInt)
    // FIXME
    //AsyncDataStream.unorderedWait(source, new AsyncDatabaseRequest, 1000, scala.concurrent.duration.MILLISECONDS)
  }

  /**
   * An implementation of the 'AsyncFunction' that sends requests and sets the callback.
   */
  class AsyncDatabaseRequest extends AsyncFunction[Int, (Int, Int)] {


    /** The database specific client that can issue concurrent requests with callbacks */
    lazy val client = None

    /** The context used for the future callbacks */
    implicit lazy val executor: ExecutionContext = ExecutionContext.fromExecutor(Executors.directExecutor())


    override def asyncInvoke(str: Int, resultFuture: ResultFuture[(Int, Int)]): Unit = {

      // issue the asynchronous request, receive a future for the result
      val resultFutureRequested: Future[Int] = Future.apply(1)

      // set the callback to be executed once the request by the client is complete
      // the callback simply forwards the result to the result future
      resultFutureRequested.onSuccess {
        case result: Int => resultFuture.complete(util.Arrays.asList((str, result)))
      }
    }
  }
}
