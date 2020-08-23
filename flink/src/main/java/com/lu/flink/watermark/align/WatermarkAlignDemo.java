package com.lu.flink.watermark.align;

import com.lu.util.DateUtil;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class WatermarkAlignDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        environment.setParallelism(1);

        DataStream<Tuple3<String, LocalDateTime, Long>> source = environment.socketTextStream("localhost", 9999)
                .map(message -> {
                    String[] strings = message.split(",");
                    LocalDateTime time = LocalDateTime.parse(strings[1]);
                    long timestamp = time.toInstant(ZoneOffset.of("+8")).toEpochMilli();
                    return Tuple3.of(strings[0], time, timestamp);
                })
                .returns(Types.TUPLE(Types.STRING, Types.LOCAL_DATE_TIME, Types.LONG));

        // 1.11 generate water
        WatermarkStrategy<Tuple3<String, LocalDateTime, Long>> strategy = WatermarkStrategy
                // 指定延时时间
                .<Tuple3<String, LocalDateTime, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                // 指定event time
                .withTimestampAssigner((tuple3, timestamp) -> tuple3.f2);

        WatermarkStrategy<Tuple3<String, LocalDateTime, Long>> watermarkStrategy = new WatermarkStrategy<Tuple3<String, LocalDateTime, Long>>() {
            @Override
            public WatermarkGenerator<Tuple3<String, LocalDateTime, Long>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new WatermarkGenerator<Tuple3<String, LocalDateTime, Long>>() {
                    private final long outOfOrdernessMillis = 3000L;
                    private long currentMaxTimestamp = Long.MIN_VALUE + outOfOrdernessMillis;
                    private Watermark currentWaterMark;

                    // 每条消息都生成watermark
                    @Override
                    public void onEvent(Tuple3<String, LocalDateTime, Long> event, long eventTimestamp, WatermarkOutput output) {
                        currentMaxTimestamp = Math.max(currentMaxTimestamp, eventTimestamp);
                        currentWaterMark = new Watermark(currentMaxTimestamp - outOfOrdernessMillis);
                        System.out.printf("%s, watermark { timestamp=%s, date=%s | currentMaxTimestamp=%s, date=%s | Watermark=%s, date=%s }%n",
                                Thread.currentThread().getName(),
                                event.f2,
                                DateUtil.transFormat(event.f2),
                                currentMaxTimestamp,
                                DateUtil.transFormat(currentMaxTimestamp),
                                currentWaterMark.getTimestamp(),
                                DateUtil.transFormat(currentWaterMark.getTimestamp())
                        );
                        output.emitWatermark(currentWaterMark);
                    }

                    // 周期性生成watermark
                    @Override
                    public void onPeriodicEmit(WatermarkOutput output) {
                        // output.emitWatermark(new Watermark(currentMaxTimestamp - outOfOrdernessMillis));
                    }
                };
            }
        }
        .withTimestampAssigner((tuple3, timestamp) -> tuple3.f2);

        source
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy(tuple3 -> tuple3.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .trigger(CustomEventTimeTrigger.create())
                .process(new ProcessWindowFunction<Tuple3<String, LocalDateTime, Long>, Tuple3<String, LocalDateTime, Long>, String, TimeWindow>() {
                    @Override
                    public void process(String key,
                                        Context context,
                                        Iterable<Tuple3<String, LocalDateTime, Long>> elements,
                                        Collector<Tuple3<String, LocalDateTime, Long>> out) throws Exception {
                        System.out.printf("%s>> TimeWindow {start=%s, end=%s} | Watermark=%s, date=%s | ProcessingTime=%s, date=%s%n",
                                Thread.currentThread().getId(),
                                DateUtil.transFormat(context.window().getStart()),
                                DateUtil.transFormat(context.window().getEnd()),
                                context.currentWatermark(),
                                DateUtil.transFormat(context.currentWatermark()),
                                context.currentProcessingTime(),
                                DateUtil.transFormat(context.currentProcessingTime())
                        );
                        for (Tuple3<String, LocalDateTime, Long> element : elements) {
                            out.collect(element);
                        }
                    }
                })
                .print();

        environment.execute();
    }
}
