package com.lu.flink.datastream.event.time;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class EventDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Tuple2<String, Long>> source = environment.socketTextStream("localhost", 9999)
                .map(message -> {
                    String[] strings = message.split(",");
                    return Tuple2.of(strings[0], Long.parseLong(strings[1]));
                })
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        WatermarkStrategy<Tuple2<String, Long>> strategy = WatermarkStrategy
                // 指定延时时间
                .<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                // 指定event time
                .withTimestampAssigner((tuple2, timestamp) -> tuple2.f1);

        source.assignTimestampsAndWatermarks(strategy)
                .keyBy(tuple2 -> tuple2.f0)
                .timeWindow(Time.seconds(14))
                .reduce((t1, t2) -> {
                    if (t1.f1 > t2.f1) {
                        return t1;
                    } else if (t1.f1 < t2.f1) {
                        return t2;
                    } else {
                        return t1;
                    }
                })
                .print();

        environment.execute();
    }
}
