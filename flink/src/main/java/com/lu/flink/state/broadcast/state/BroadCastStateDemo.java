package com.lu.flink.state.broadcast.state;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BroadCastStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<Integer, String>> source1 = environment
                .socketTextStream("localhost", 9999)
                .map(message -> {
                    String[] strings = message.split(",");
                    return Tuple2.of(Integer.parseInt(strings[0]), strings[1]);
                }).returns(Types.TUPLE(Types.INT, Types.STRING));
        DataStream<Tuple2<Integer, String>> broadcastSource = environment
                .socketTextStream("localhost", 9998)
                .map(message -> {
                    String[] strings = message.split(",");
                    return Tuple2.of(Integer.parseInt(strings[0]), strings[1]);
                }).returns(Types.TUPLE(Types.INT, Types.STRING));
        KeyedStream<Tuple2<Integer, String>, Integer> keyedStream = source1.keyBy(tuple2 -> tuple2.f0);

        MapStateDescriptor<Integer, Tuple2<Integer, String>> mapStateDescriptor = new MapStateDescriptor<>(
                "BroadCastState", Types.INT, TypeInformation.of(new TypeHint<Tuple2<Integer, String>>() {}));
        BroadcastStream<Tuple2<Integer, String>> broadcastStream = broadcastSource.broadcast(mapStateDescriptor);

        keyedStream.connect(broadcastStream)
                .process(new KeyedBroadcastProcessFunction<Integer, Tuple2<Integer, String>, Tuple2<Integer, String>, String>() {
                    private final MapStateDescriptor<Integer, List<Tuple2<Integer, String>>> mapStateDesc = new MapStateDescriptor<>(
                                    "items", Types.INT, new ListTypeInfo<>(Types.TUPLE(Types.INT, Types.STRING)));

                    private final MapStateDescriptor<Integer, Tuple2<Integer, String>> broadCastState = new MapStateDescriptor<>(
                            "BroadCastState", Types.INT, TypeInformation.of(new TypeHint<Tuple2<Integer, String>>() {}));

                    @Override
                    public void processElement(Tuple2<Integer, String> value, ReadOnlyContext ctx,
                                               Collector<String> out) throws Exception {
                        MapState<Integer, List<Tuple2<Integer, String>>> mapState = getRuntimeContext().getMapState(mapStateDesc);
                        Integer id = value.f0;
                        for (Map.Entry<Integer, Tuple2<Integer, String>> entry : ctx.getBroadcastState(broadCastState).immutableEntries()) {
                            Integer broadcastId = entry.getKey();
                            Tuple2<Integer, String> broadcastValue = entry.getValue();

                            List<Tuple2<Integer, String>> stateTupleList = mapState.get(broadcastId);
                            if (stateTupleList == null) {
                                stateTupleList = new ArrayList<>();
                            }
                            // System.out.println(value + " -- " + broadcastValue + " -- ");
                            // stateTupleList.forEach(System.out::println);
                            if (id.equals(broadcastValue.f0) && !stateTupleList.isEmpty()) {
                                for (Tuple2<Integer, String> tuple2 : stateTupleList) {
                                    out.collect("MATCH：" + tuple2 + "-" + value);
                                }
                                stateTupleList.clear();
                            }

                            if (id.equals(broadcastId)) {
                                stateTupleList.add(value);
                            }
                            if (stateTupleList.isEmpty()) {
                                mapState.remove(broadcastId);
                            } else {
                                mapState.put(broadcastId, stateTupleList);
                            }
                        }
                    }

                    @Override
                    public void processBroadcastElement(Tuple2<Integer, String> value, Context ctx,
                                                        Collector<String> out) throws Exception {
                        ctx.getBroadcastState(broadCastState).put(value.f0, value);
                    }
                })
                .print();

        environment.execute();
    }
}
