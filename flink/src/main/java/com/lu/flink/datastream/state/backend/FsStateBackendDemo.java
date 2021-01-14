package com.lu.flink.datastream.state.backend;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.formats.json.JsonNodeDeserializationSchema;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class FsStateBackendDemo {
    public static void main(String[] args) throws Exception {
        // TODO start with checkpoint
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.enableCheckpointing(100);
        String checkpointDirectory = null;
        if (args.length > 0) {
            checkpointDirectory = args[0];
        }
        StateBackend stateBackend = new FsStateBackend(checkpointDirectory);
        environment.setStateBackend(stateBackend);
        environment.setParallelism(2);
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9095");
        properties.put("group.id", "kafka-state-demo-group");
        FlinkKafkaConsumer<ObjectNode> ratesHistoryConsumer = new FlinkKafkaConsumer<>("fs-state-backend", new JsonNodeDeserializationSchema(), properties);
        environment.addSource(ratesHistoryConsumer)
                .map(node -> Tuple2.of(node.get("id").intValue(), 1))
                .returns(Types.TUPLE(Types.INT, Types.INT))
                .keyBy(tuple -> tuple.f0)
                .reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> value1, Tuple2<Integer, Integer> value2) throws Exception {
                        int o = value1.f1 + value2.f1;
                        Thread.sleep(100);
                        return Tuple2.of(value1.f0, o);
                    }
                })
                .print();
        environment.execute();
    }
}
