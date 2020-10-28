package com.lu.flink.datastream.operators.window.join.interval;

import com.lu.flink.datastream.operators.window.join.MyTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class IntervalJoinWindowJoinDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        environment.setParallelism(2);

        WatermarkStrategy<Tuple2<Integer, Long>> timestampAssigner = new MyTimestampAssigner()
                .withTimestampAssigner((tuple2, timestamp) -> tuple2.f1);

        DataStream<Tuple2<Integer, Long>> orangeStream = environment.socketTextStream("localhost", 9999)
                .map(message -> {
                    String[] strings = message.split(",");
                    return Tuple2.of(Integer.parseInt(strings[0]), LocalDateTime.parse(strings[1])
                            .toInstant(ZoneOffset.of("+8")).toEpochMilli());
                })
                .returns(Types.TUPLE(Types.INT, Types.LONG))
                .assignTimestampsAndWatermarks(timestampAssigner);

        DataStream<Tuple2<Integer, Long>> greenStream = environment.socketTextStream("localhost", 9998)
                .map(message -> {
                    String[] strings = message.split(",");
                    return Tuple2.of(Integer.parseInt(strings[0]), LocalDateTime.parse(strings[1])
                            .toInstant(ZoneOffset.of("+8")).toEpochMilli());
                })
                .returns(Types.TUPLE(Types.INT, Types.LONG))
                .assignTimestampsAndWatermarks(timestampAssigner);

        orangeStream
                .keyBy(tuple2 -> tuple2.f0)
                .intervalJoin(greenStream.keyBy(tuple2 -> tuple2.f0))
                .between(Time.seconds(-2), Time.seconds(2))
                .process(new ProcessJoinFunction<Tuple2<Integer, Long>, Tuple2<Integer, Long>, String>() {
                    @Override
                    public void processElement(Tuple2<Integer, Long> left, Tuple2<Integer, Long> right, Context ctx,
                                               Collector<String> out) throws Exception {

                        out.collect(left.f0 + "," + left.f1 + "," + right.f1 + "[" + ctx.getLeftTimestamp()
                                + ", " + ctx.getRightTimestamp() + "]");
                    }
                })
                .print();

        environment.execute();
    }
}
