package com.lu.flink.datastream.state.backend;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.formats.json.JsonNodeDeserializationSchema;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.time.LocalDateTime;
import java.util.Properties;

public class FsStateBackendDemo {
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        String checkpointDirectory = parameterTool.get("check", "file:///" + System.getProperty("user.dir") + "/flink/src/main/resources/checkpoints");
        StateBackend stateBackend = new FsStateBackend(checkpointDirectory);

        CheckpointConfig checkpointConfig = environment.getCheckpointConfig();
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);

        environment.enableCheckpointing(1000);
        environment.setParallelism(1);
        environment.setStateBackend(stateBackend);

        LocalDateTime now = LocalDateTime.now();
        LocalDateTime upper = now.plusSeconds(3 * 60);
        LocalDateTime lower = now.plusSeconds(40);
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
                        Tuple2<Integer, Integer> result = Tuple2.of(value1.f0, o);
                        System.out.println("result: " + result);
                        LocalDateTime time = LocalDateTime.now();
                        // TODO 查看是否有状态
                        if (result.f0 == 2 && time.isBefore(upper)) {
                            throw new Exception(time + " < " + upper);
                        }
                        Thread.sleep(10 * 1000);
                        return result;
                    }
                })
                .print();
        environment.execute();
    }
}
