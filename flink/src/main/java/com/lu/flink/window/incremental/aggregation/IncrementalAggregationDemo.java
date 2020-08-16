package com.lu.flink.window.incremental.aggregation;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class IncrementalAggregationDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        environment.socketTextStream("localhost", 9999)
                .map(message -> {
                    String[] strings = message.split(",");
                    return Tuple3.of(strings[0], Long.parseLong(strings[1]), Integer.parseInt(strings[2]));
                })
                .returns(Types.TUPLE(Types.STRING, Types.LONG, Types.INT))
                .keyBy(tuple2 -> tuple2.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
                .reduce((t1, t2) -> (t1.f2 > t2.f2 ? t1 : t2), new MyWindowFunction())
                .print();

        environment.execute();
    }
}
