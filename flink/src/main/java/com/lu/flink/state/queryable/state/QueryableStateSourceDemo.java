package com.lu.flink.state.queryable.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.QueryableStateOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class QueryableStateSourceDemo {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(RestOptions.PORT, 8081);
        configuration.set(QueryableStateOptions.ENABLE_QUERYABLE_STATE_PROXY_SERVER, true);
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        environment
                .socketTextStream("localhost", 9999)
                .map(message -> {
                    String[] strings = message.split(",");
                    return Tuple2.of(Integer.parseInt(strings[0]), Integer.parseInt(strings[1]));
                })
                .returns(Types.TUPLE(Types.INT, Types.INT))
                .keyBy(tuple2 -> tuple2.f0)
                .flatMap(new RichFlatMapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
                    private transient ValueState<Tuple2<Integer, Integer>> sum;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<Tuple2<Integer, Integer>> descriptor =
                                new ValueStateDescriptor<>(
                                        "average", // the state name
                                        Types.TUPLE(Types.INT, Types.INT)); // type information
                        descriptor.setQueryable("query-name");
                        sum = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public void flatMap(Tuple2<Integer, Integer> value,
                                        Collector<Tuple2<Integer, Integer>> out) throws Exception {
                        Tuple2<Integer, Integer> currentSum = sum.value();
                        if (null == currentSum) {
                            currentSum = Tuple2.of(0, 0);
                        }
                        currentSum.f0 += 1;
                        currentSum.f1 += value.f1;
                        sum.update(currentSum);

                        if (currentSum.f0 >= 2) {
                            out.collect(Tuple2.of(value.f0, currentSum.f1 / currentSum.f0));
                            sum.clear();
                        }
                    }
                })
                .print();

        environment.execute();
    }
}
