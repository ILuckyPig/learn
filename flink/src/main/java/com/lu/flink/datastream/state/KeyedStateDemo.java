package com.lu.flink.datastream.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeyedStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = environment.socketTextStream("localhost", 9999);
        source
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String s) throws Exception {
                        String[] strings = s.split(",");
                        return Tuple2.of(strings[0], Integer.valueOf(strings[1]));
                    }
                })
                .keyBy(tuple2 -> tuple2.f0)
                .reduce(new RichReduceFunction<Tuple2<String, Integer>>() {
                    MapState<String, Integer> mapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        MapStateDescriptor<String, Integer> mapStateDescriptor = new MapStateDescriptor<>("mapState", String.class, Integer.class);
                        mapState = getRuntimeContext().getMapState(mapStateDescriptor);
                    }

                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
                        int i;
                        if (mapState.contains(t1.f0)) {
                            i = mapState.get(t1.f0);
                        } else {
                            i = t2.f1;
                        }
                        return Tuple2.of(t1.f0, t1.f1 + i);
                    }
                })
                .print();
        environment.execute();
    }
}
