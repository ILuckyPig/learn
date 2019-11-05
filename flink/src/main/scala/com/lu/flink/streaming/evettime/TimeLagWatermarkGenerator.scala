package com.lu.flink.streaming.evettime

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

/**
 * 基于系统时间
 */
class TimeLagWatermarkGenerator extends AssignerWithPeriodicWatermarks[String] {
  val maxTimeLong = 3000;

  override def getCurrentWatermark: Watermark = {
    new Watermark(System.currentTimeMillis() - maxTimeLong)
  }

  override def extractTimestamp(t: String, l: Long): Long = {
    t.toLong
  }
}
