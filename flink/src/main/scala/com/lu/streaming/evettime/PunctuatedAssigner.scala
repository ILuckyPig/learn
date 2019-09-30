package com.lu.streaming.evettime

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

/**
 * 对某一个特定event创建watermark
 * 首先调用extractTimestamp分配timestamp
 * 然后调用checkAndGetNextWatermark决定是否生成watermark
 */
class PunctuatedAssigner extends AssignerWithPunctuatedWatermarks[String] {
  override def checkAndGetNextWatermark(t: String, l: Long): Watermark = {
    // simply emit a watermark with every event
    new Watermark(l - 3000)
  }

  override def extractTimestamp(t: String, l: Long): Long = {
    t.toLong
  }
}
