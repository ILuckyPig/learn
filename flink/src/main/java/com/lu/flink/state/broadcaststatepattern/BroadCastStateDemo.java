package com.lu.flink.state.broadcaststatepattern;

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
        // TODO can output
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<Integer, String>> source1 = environment.fromElements(Tuple2.of(1, "a"), Tuple2.of(2, "b"), Tuple2.of(3, "c"));
        DataStream<Tuple2<Integer, String>> source2 = environment.fromElements(Tuple2.of(1, "张三"), Tuple2.of(2, "李四"));
        KeyedStream<Tuple2<Integer, String>, Integer> keyedStream = source1.keyBy(tuple2 -> tuple2.f0);

        MapStateDescriptor<Integer, Tuple2<Integer, String>> mapStateDescriptor = new MapStateDescriptor<>(
                "BroadCastState", Types.INT, TypeInformation.of(new TypeHint<Tuple2<Integer, String>>() {}));
        BroadcastStream<Tuple2<Integer, String>> broadcastStream = source2.broadcast(mapStateDescriptor);

        keyedStream.connect(broadcastStream)
                .process(new KeyedBroadcastProcessFunction<Integer, Tuple2<Integer, String>, Tuple2<Integer, String>, String>() {
                    // store partial matches, i.e. first elements of the pair waiting for their second element
                    // we keep a list as we may have many first elements waiting
                    private final MapStateDescriptor<Integer, List<Tuple2<Integer, String>>> mapStateDesc = new MapStateDescriptor<>(
                                    "items", Types.INT, new ListTypeInfo<>(Types.TUPLE(Types.INT, Types.STRING)));

                    // identical to our ruleStateDescriptor above
                    private final MapStateDescriptor<Integer, Tuple2<Integer, String>> mapStateDescriptor = new MapStateDescriptor<>(
                            "BroadCastState", Types.INT, TypeInformation.of(new TypeHint<Tuple2<Integer, String>>() {}));

                    @Override
                    public void processElement(Tuple2<Integer, String> value, ReadOnlyContext ctx,
                                               Collector<String> out) throws Exception {
                        final MapState<Integer, List<Tuple2<Integer, String>>> mapState = getRuntimeContext().getMapState(mapStateDesc);
                        Integer id = value.f0;
                        for (Map.Entry<Integer, Tuple2<Integer, String>> entry : ctx.getBroadcastState(mapStateDescriptor).immutableEntries()) {
                            Integer broadcastId = entry.getKey();
                            Tuple2<Integer, String> broadcastValue = entry.getValue();

                            List<Tuple2<Integer, String>> stateTuple2 = mapState.get(broadcastId);
                            if (stateTuple2 == null) {
                                stateTuple2 = new ArrayList<>();
                            }

                            if (id.equals(broadcastValue.f0) && !stateTuple2.isEmpty()) {
                                for (Tuple2<Integer, String> tuple2 : stateTuple2) {
                                    out.collect("MATCH：" + tuple2 + "-" + value);
                                }
                                stateTuple2.clear();
                            }

                            if (id.equals(broadcastId)) {
                                stateTuple2.add(value);
                            }
                            if (stateTuple2.isEmpty()) {
                                mapState.remove(broadcastId);
                            } else {
                                mapState.put(broadcastId, stateTuple2);
                            }

                        }
                    }

                    @Override
                    public void processBroadcastElement(Tuple2<Integer, String> value, Context ctx,
                                                        Collector<String> out) throws Exception {
                        ctx.getBroadcastState(mapStateDescriptor).put(value.f0, value);
                    }
                })
                .print();

        environment.execute();
    }
}
