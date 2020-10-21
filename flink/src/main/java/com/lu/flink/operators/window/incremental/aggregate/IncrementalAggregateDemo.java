package com.lu.flink.operators.window.incremental.aggregate;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class IncrementalAggregateDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        environment.socketTextStream("localhost", 9999)
                .map(message -> {
                    String[] strings = message.split(",");
                    return Tuple2.of(strings[0], Long.parseLong(strings[1]));
                })
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(tuple2 -> tuple2.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
                .aggregate(new AverageAggregate(),
                        new ProcessWindowFunction<Double, Tuple2<String, Double>, String, TimeWindow>() {

                            @Override
                            public void process(String key, Context context, Iterable<Double> averages,
                                                Collector<Tuple2<String, Double>> out) throws Exception {
                                Double next = averages.iterator().next();
                                out.collect(Tuple2.of(key, next));
                            }
                        }
                )
                .print();

        environment.execute();
    }
}
