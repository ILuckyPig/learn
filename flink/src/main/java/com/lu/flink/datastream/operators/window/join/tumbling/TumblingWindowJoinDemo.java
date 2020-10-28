package com.lu.flink.datastream.operators.window.join.tumbling;

import com.lu.flink.datastream.operators.window.join.MyTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class TumblingWindowJoinDemo implements Serializable {
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

        orangeStream.join(greenStream)
                .where(tuple2 -> tuple2.f0)
                .equalTo(tuple2 -> tuple2.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new JoinFunction<Tuple2<Integer, Long>, Tuple2<Integer, Long>, String>() {

                    @Override
                    public String join(Tuple2<Integer, Long> first, Tuple2<Integer, Long> second) throws Exception {
                        return first.f0 + ", " + first.f1 + "," + second.f1;
                    }
                }).print();

        environment.execute();
}
}
