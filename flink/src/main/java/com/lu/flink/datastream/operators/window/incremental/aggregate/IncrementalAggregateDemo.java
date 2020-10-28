package com.lu.flink.datastream.operators.window.incremental.aggregate;

import org.apache.flink.api.common.functions.AggregateFunction;
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

    public static class AverageAggregate implements AggregateFunction<Tuple2<String, Long>, Tuple2<Long, Long>, Double> {
        @Override
        public Tuple2<Long, Long> createAccumulator() {
            return Tuple2.of(0L, 0L);

        }

        @Override
        public Tuple2<Long, Long> add(Tuple2<String, Long> value, Tuple2<Long, Long> accumulator) {
            return Tuple2.of(accumulator.f0 + value.f1, accumulator.f1 + 1L);
        }

        @Override
        public Double getResult(Tuple2<Long, Long> accumulator) {
            return ((double) accumulator.f0) / accumulator.f1;
        }

        @Override
        public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
            return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
        }
    }
}
