package com.lu.flink.process.function;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

public class KeyedCoProcessFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> source1 = environment.fromElements(Tuple2.of("a", 1), Tuple2.of("b", 1), Tuple2.of("c", 1));
        DataStream<Tuple2<String, Integer>> source2 = environment.fromElements(Tuple2.of("a", 10), Tuple2.of("c", 12));

        source1
                .keyBy(tuple2 -> tuple2.f0)
                .connect(source2.keyBy(tuple2 -> tuple2.f0))
                .process(new KeyedCoProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple3<String, Integer, Integer>>() {
                    MapState<String, Integer> mapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        MapStateDescriptor<String, Integer> mapStateDescriptor = new MapStateDescriptor<String, Integer>("source1State", String.class, Integer.class);
                        mapState = getRuntimeContext().getMapState(mapStateDescriptor);
                    }

                    @Override
                    public void processElement1(Tuple2<String, Integer> value, Context ctx, Collector<Tuple3<String, Integer, Integer>> out) throws Exception {
                        if (!mapState.contains(value.f0)) {
                            mapState.put(value.f0, value.f1);
                        }
                    }

                    @Override
                    public void processElement2(Tuple2<String, Integer> value, Context ctx, Collector<Tuple3<String, Integer, Integer>> out) throws Exception {
                        if (mapState.contains(value.f0)) {
                            out.collect(Tuple3.of(value.f0, mapState.get(value.f0), value.f1));
                        }
                    }
                })
                .print();

        environment.execute();
    }
}
