package com.lu.flink.datastream.backpressure;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.LocalDateTime;

/**
 * TODO test
 */
public class BackPressureDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.disableOperatorChaining();
        environment
                .socketTextStream("localhost", 10234)
                .name("source")
                .map(new MapFunction<String, Tuple2<Integer, LocalDateTime>>() {
                    @Override
                    public Tuple2<Integer, LocalDateTime> map(String s) throws Exception {
                        return Tuple2.of(Integer.parseInt(s.split(",")[0]),
                                LocalDateTime.parse(s.split(",")[1]));
                    }
                })
                .name("map-1")
                .map(new MapFunction<Tuple2<Integer, LocalDateTime>, Tuple2<Integer, LocalDateTime>>() {
                    @Override
                    public Tuple2<Integer, LocalDateTime> map(Tuple2<Integer, LocalDateTime> tuple2) throws Exception {
                        Thread.sleep(30 * 1000);
                        return tuple2;
                    }
                })
                .name("map-2")
                .map(new MapFunction<Tuple2<Integer, LocalDateTime>, Tuple2<Integer, LocalDateTime>>() {
                    @Override
                    public Tuple2<Integer, LocalDateTime> map(Tuple2<Integer, LocalDateTime> tuple2) throws Exception {
                        return tuple2;
                    }
                })
                .name("map-3")
                .print();
        environment.execute();
    }
}
