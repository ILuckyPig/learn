package com.lu.flink.datastream.operators.window.late.events;

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
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * key,2020-08-25T17:28:00
 * key,2020-08-25T17:28:13
 * --> (key,2020-08-25T17:28:00,1598347680000) 触发 [2020-08-25 17:28:00, 2020-08-25 17:28:10)的窗口
 * key,2020-08-25T17:28:23
 * --> (key,2020-08-25T17:28:13,1598347693000) 触发 [2020-08-25 17:28:10, 2020-08-25 17:28:20)的窗口
 * key,2020-08-25T17:28:01
 * --> (late,2020-08-25T17:28:01,1598347681000) 迟到
 * key,2020-08-25T17:28:02
 * --> (late,2020-08-25T17:28:02,1598347682000) 迟到
 * key,2020-08-25T17:28:15
 * --> (late,2020-08-25T17:28:15,1598347695000) 迟到
 */
public class LateEeventsDemo {
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

        // add side output process late events
        OutputTag<Tuple3<String, LocalDateTime, Long>> lateTag = new OutputTag<Tuple3<String, LocalDateTime, Long>>("late"){};

        SingleOutputStreamOperator<Tuple3<String, LocalDateTime, Long>> process = source
                .keyBy(tuple3 -> tuple3.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .sideOutputLateData(lateTag)
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

        // late events side out
        // 迟到的数据会通过这里打印出来
        process
                .getSideOutput(lateTag)
                .map(tuple3 -> Tuple3.of("late", tuple3.f1, tuple3.f2))
                .returns(Types.TUPLE(Types.STRING, Types.LOCAL_DATE_TIME, Types.LONG))
                .print();
        process
                .print();

        environment.execute();
    }
}
