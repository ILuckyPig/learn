package com.lu.flink.window.late.events;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class LateEvents {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Tuple3<String, LocalDateTime, Long>> source = environment.socketTextStream("localhost", 9999)
                .map(message -> {
                    String[] strings = message.split(",");
                    LocalDateTime time = LocalDateTime.parse(strings[1]);
                    long timestamp = time.toInstant(ZoneOffset.of("+8")).getEpochSecond();
                    return Tuple3.of(strings[0], time, timestamp);
                })
                .returns(Types.TUPLE(Types.STRING, Types.LOCAL_DATE_TIME, Types.LONG));

        WatermarkStrategy<Tuple3<String, LocalDateTime, Long>> strategy = WatermarkStrategy
                // 指定延时时间
                .<Tuple3<String, LocalDateTime, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                // 指定event time
                .withTimestampAssigner((tuple3, timestamp) -> tuple3.f2);

        source.assignTimestampsAndWatermarks(strategy)
                .keyBy(tuple3 -> tuple3.f0)
                .timeWindow(Time.seconds(14))
                .process(new ProcessWindowFunction<Tuple3<String, LocalDateTime, Long>, Tuple3<String, LocalDateTime, Long>, String, TimeWindow>() {
                    @Override
                    public void process(String key,
                                        Context context,
                                        Iterable<Tuple3<String, LocalDateTime, Long>> elements,
                                        Collector<Tuple3<String, LocalDateTime, Long>> out) throws Exception {
                        System.out.println(key + ", " + context.window());
                        for (Tuple3<String, LocalDateTime, Long> element : elements) {
                            out.collect(element);
                        }
                    }
                })
                // TODO can not print any one element
                .print();

        environment.execute();
    }
}
