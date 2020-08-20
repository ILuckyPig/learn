package com.lu.flink.window.late.events;

import com.lu.util.DateUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class LateEvents {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

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

        // TODO 自定义时间戳不能触发窗口
        source
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple3<String, LocalDateTime, Long>>() {
                    private long currentMaxTimestamp = Long.MIN_VALUE;

                    private long outOfOrdernessMillis = 3000L;

                    private Watermark currentWaterMark;

                    @Override
                    public Watermark getCurrentWatermark() {
                        currentWaterMark = new Watermark(currentMaxTimestamp - outOfOrdernessMillis - 1);
                        return currentWaterMark;
                    }

                    @Override
                    public long extractTimestamp(Tuple3<String, LocalDateTime, Long> element, long recordTimestamp) {
                        currentMaxTimestamp = Math.max(currentMaxTimestamp, element.f2);
                        System.out.printf("timestamp=%s, date=%s | currentMaxTimestamp=%s, currentMaxDate=%s | %s%n",
                                element.f2,
                                DateUtil.trans2LocalDateTime(element.f2).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                                currentMaxTimestamp,
                                DateUtil.trans2LocalDateTime(currentMaxTimestamp).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                                currentWaterMark.toString()
                        );
                        return currentMaxTimestamp;
                    }
                })
                .keyBy(tuple3 -> tuple3.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(15)))
                .process(new ProcessWindowFunction<Tuple3<String, LocalDateTime, Long>, Tuple3<String, LocalDateTime, Long>, String, TimeWindow>() {
                    @Override
                    public void process(String key,
                                        Context context,
                                        Iterable<Tuple3<String, LocalDateTime, Long>> elements,
                                        Collector<Tuple3<String, LocalDateTime, Long>> out) throws Exception {
                        System.out.printf("TimeWindow{start=%s, end=%s}, Watermark=%s, ProcessingTime=%s%n",
                                DateUtil.trans2LocalDateTime(context.window().getStart()).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                                DateUtil.trans2LocalDateTime(context.window().getEnd()).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                                context.currentWatermark(),
                                context.currentProcessingTime()
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
