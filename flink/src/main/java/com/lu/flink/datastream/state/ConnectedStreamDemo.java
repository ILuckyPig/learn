package com.lu.flink.datastream.state;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

public class ConnectedStreamDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> control = environment.fromElements("DROP", "IGNORE").keyBy(x -> x);
        DataStream<String> streamOfWords = environment.fromElements("Apache", "DROP", "Flink", "IGNORE").keyBy(x -> x);

        control
                .connect(streamOfWords)
                .flatMap(new ControlFunction())
                .print();

        environment.execute();
    }

    static class ControlFunction extends RichCoFlatMapFunction<String, String, String> {
        private ValueState<Boolean> blocked;

        @Override
        public void open(Configuration config) {
            blocked = getRuntimeContext().getState(new ValueStateDescriptor<>("blocked", Boolean.class));
        }

        @Override
        public void flatMap1(String control_value, Collector<String> out) throws Exception {
            blocked.update(Boolean.TRUE);
        }

        @Override
        public void flatMap2(String data_value, Collector<String> out) throws Exception {
            if (blocked.value() == null) {
                out.collect(data_value);
            }
        }
    }
}
