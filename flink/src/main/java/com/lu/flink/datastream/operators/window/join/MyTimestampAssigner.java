package com.lu.flink.datastream.operators.window.join;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.java.tuple.Tuple2;

public class MyTimestampAssigner implements WatermarkStrategy<Tuple2<Integer, Long>> {
    @Override
    public WatermarkGenerator<Tuple2<Integer, Long>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new WatermarkGenerator<Tuple2<Integer, Long>>() {
            private long currentMaxTimestamp = Long.MIN_VALUE;

            @Override
            public void onEvent(Tuple2<Integer, Long> event, long eventTimestamp, WatermarkOutput output) {
                currentMaxTimestamp = Math.max(currentMaxTimestamp, event.f1);
                Watermark currentWaterMark = new Watermark(currentMaxTimestamp);
                output.emitWatermark(currentWaterMark);
            }

            @Override
            public void onPeriodicEmit(WatermarkOutput output) {

            }
        };
    }
}
