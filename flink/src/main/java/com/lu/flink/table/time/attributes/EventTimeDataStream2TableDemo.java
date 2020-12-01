package com.lu.flink.table.time.attributes;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.util.Properties;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * TODO 时区问题
 */
public class EventTimeDataStream2TableDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        environment.setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment, settings);

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "127.0.0.1:9092");
        properties.put("group.id", "kafka-table-demo-group");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("user-search-log", new SimpleStringSchema(), properties);
        consumer.setStartFromEarliest();
        DataStream<String> source = environment.addSource(consumer);
        DataStream<Tuple3<Timestamp, Integer, String>> timestampDataStream = source
                .map(new MapFunction<String, Tuple3<Timestamp, Integer, String>>() {
                    @Override
                    public Tuple3<Timestamp, Integer, String> map(String value) throws Exception {
                        String[] strings = value.split(",");
                        return Tuple3.of(Timestamp.valueOf(strings[0]), Integer.parseInt(strings[1]), strings[2]);
                    }
                })
                .assignTimestampsAndWatermarks(new WatermarkStrategy<Tuple3<Timestamp, Integer, String>>() {
                    @Override
                    public WatermarkGenerator<Tuple3<Timestamp, Integer, String>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                        return new WatermarkGenerator<Tuple3<Timestamp, Integer, String>>() {
                            private long currentMaxTimestamp = Long.MIN_VALUE;

                            @Override
                            public void onEvent(Tuple3<Timestamp, Integer, String> event, long eventTimestamp, WatermarkOutput output) {
                                currentMaxTimestamp = Math.max(event.f0.getTime(), currentMaxTimestamp);
                                output.emitWatermark(new Watermark(currentMaxTimestamp));
                            }

                            @Override
                            public void onPeriodicEmit(WatermarkOutput output) {

                            }
                        };
                    }
                }.withTimestampAssigner((tuple3, timestamp) -> tuple3.f0.getTime()));

        Table table = tableEnvironment.fromDataStream(timestampDataStream, $("log_time").rowtime(), $("id"), $("word"));

        Table result = table
                .window(Tumble
                        .over(lit(10).seconds())
                        .on($("log_time"))
                        .as("w"))
                .groupBy($("id"), $("w"))
                .select($("id"), $("w").end(), $("word").count());

        tableEnvironment.toAppendStream(result, Row.class).print();

        environment.execute();
    }
}
