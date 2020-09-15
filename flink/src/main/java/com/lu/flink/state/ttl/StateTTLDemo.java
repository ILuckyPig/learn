package com.lu.flink.state.ttl;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StateTTLDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> source = environment.socketTextStream("localhost", 9999);

        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.seconds(10))
                // .disableCleanupInBackground() // 禁用后台清除状态
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                // .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                // .setUpdateType(StateTtlConfig.UpdateType.Disabled) // ttl被禁用，state不会过期
                // .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired) // 不会返回已经过期的value
                .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp) // 如果过期value没有被清除，会返回过期的value
                .build();

        source
                .map(message -> {
                    String[] strings = message.split(",");
                    return Tuple2.of(strings[0], Integer.parseInt(strings[1]));
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(tuple2 -> tuple2.f0)
                .map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                    private transient ValueState<Integer> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>("map state", Integer.class);
                        descriptor.enableTimeToLive(ttlConfig);
                        valueState = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                        int sum;
                        if (null != valueState.value()) {
                            sum = value.f1 + valueState.value();
                        } else {
                            sum = value.f1;
                        }
                        valueState.update(sum);
                        return Tuple2.of(value.f0, sum);
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .print();

        environment.execute();
    }
}
