package com.lu.streaming.evettime

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

/**
 * 基于系统时间的自定义周期性watermark
 */
class TimeLagWatermarkGenerator extends AssignerWithPeriodicWatermarks[String] {

  val maxTimeLong = 3000;

  var currentMaxTimestamp: Long = _

  override def getCurrentWatermark: Watermark = {
    new Watermark(currentMaxTimestamp - maxTimeLong)
  }

  override def extractTimestamp(t: String, l: Long): Long = {
    val timestamp = t.toLong
    currentMaxTimestamp = Math.max(currentMaxTimestamp, timestamp)
    println("message: " + t + ",eventtime:" + timestamp + ",currentMaxTimestamp:" + currentMaxTimestamp+",watermark:" + getCurrentWatermark)
    timestamp
  }
}
