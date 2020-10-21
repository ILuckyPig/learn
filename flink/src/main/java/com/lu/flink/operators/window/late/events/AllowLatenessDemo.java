package com.lu.flink.operators.window.late.events;

import com.lu.util.DateUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * key,2020-08-25T19:21:20
 * key,2020-08-25T19:21:33
 * --> (key,2020-08-25T19:21:20,1598354480000) 触发 [2020-08-25 19:21:20, 2020-08-25 19:21:30) 的窗口
 * key,2020-08-25T19:21:21
 * --> (key,2020-08-25T19:21:20,1598354480000) 再次触发 [2020-08-25 19:21:20, 2020-08-25 19:21:30) 的窗口
 * --> (key,2020-08-25T19:21:21,1598354481000)
 *
 * [window.start, window.end + delay)
 * allowedLateness
 */
public class AllowLatenessDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        environment.setParallelism(1);

        // 1.11 generate water
        WatermarkStrategy<Tuple3<String, LocalDateTime, Long>> strategy = WatermarkStrategy
                // 指定延时时间
                .<Tuple3<String, LocalDateTime, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                // 指定event time
                .withTimestampAssigner((tuple3, timestamp) -> tuple3.f2);

        DataStream<Tuple3<String, LocalDateTime, Long>> source = environment
                .socketTextStream("localhost", 9999)
                .map(message -> {
                    String[] strings = message.split(",");
                    LocalDateTime time = LocalDateTime.parse(strings[1]);
                    long timestamp = time.toInstant(ZoneOffset.of("+8")).toEpochMilli();
                    return Tuple3.of(strings[0], time, timestamp);
                })
                .returns(Types.TUPLE(Types.STRING, Types.LOCAL_DATE_TIME, Types.LONG))
                .assignTimestampsAndWatermarks(strategy);

        SingleOutputStreamOperator<Tuple3<String, LocalDateTime, Long>> process = source
                .keyBy(tuple3 -> tuple3.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                // 窗口允许迟到3秒
                // 窗口触发计算后，3秒内如果还有属于该窗口的数据进入，会再次触发窗口
                .allowedLateness(Time.seconds(3))
                .process(new ProcessWindowFunction<Tuple3<String, LocalDateTime, Long>, Tuple3<String, LocalDateTime, Long>, String, TimeWindow>() {
                    @Override
                    public void process(String s,
                                        Context context,
                                        Iterable<Tuple3<String, LocalDateTime, Long>> elements,
                                        Collector<Tuple3<String, LocalDateTime, Long>> out) throws Exception {
                        for (Tuple3<String, LocalDateTime, Long> element : elements) {
                            System.out.printf("window {start=%s, end=%s}%n",
                                    DateUtil.transFormat(context.window().getStart()),
                                    DateUtil.transFormat(context.window().getEnd())
                            );
                            out.collect(element);
                        }
                    }
                });

        process
                .print();

        environment.execute();
    }
}
