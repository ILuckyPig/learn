package com.lu.flink.table.temporal.table;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.util.Properties;

import static org.apache.flink.table.api.Expressions.$;

public class TemporalTableDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        environment.enableCheckpointing(10 * 1000);
        environment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        environment.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        environment.setParallelism(2);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment, settings);

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "127.0.0.1:9092");
        properties.put("group.id", "kafka-table-demo-group");
        FlinkKafkaConsumer<String> ordersConsumer = new FlinkKafkaConsumer<>("Orders", new SimpleStringSchema(), properties);
        ordersConsumer.setStartFromEarliest();
        FlinkKafkaConsumer<String> ratesHistoryConsumer = new FlinkKafkaConsumer<>("RatesHistory", new SimpleStringSchema(), properties);
        ratesHistoryConsumer.setStartFromEarliest();

        tableEnvironment.executeSql(
                "CREATE TABLE orders (" +
                        "   order_time TIMESTAMP(3)," +
                        "   amount INT," +
                        "   currency STRING," +
                        "   WATERMARK FOR order_time AS order_time" +
                        ") WITH (" +
                        "   'connector' = 'kafka'," +
                        "   'topic' = 'Orders'," +
                        "   'properties.bootstrap.servers' = '127.0.0.1:9092'," +
                        "   'properties.group.id' = 'kafka-table-demo-group'," +
                        "   'format' = 'csv'," +
                        "   'csv.field-delimiter' = ','," +
                        "   'scan.startup.mode' = 'earliest-offset'" +
                        ")");

        DataStream<Tuple3<Timestamp, String, Integer>> rateHistory = environment
                .addSource(ratesHistoryConsumer)
                .map(message -> {
                    String[] split = message.split(",");
                    return Tuple3.of(Timestamp.valueOf(split[0]), split[1], Integer.valueOf(split[2]));
                })
                .returns(Types.TUPLE(Types.SQL_TIMESTAMP, Types.STRING, Types.INT))
                .assignTimestampsAndWatermarks(new WatermarkStrategy<Tuple3<Timestamp, String, Integer>>() {
                    @Override
                    public WatermarkGenerator<Tuple3<Timestamp, String, Integer>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                        return new WatermarkGenerator<Tuple3<Timestamp, String, Integer>>() {
                            private long currentMaxTimestamp = Long.MIN_VALUE;

                            @Override
                            public void onEvent(Tuple3<Timestamp, String, Integer> event, long eventTimestamp, WatermarkOutput output) {
                                currentMaxTimestamp = Math.max(event.f0.getTime(), currentMaxTimestamp);
                                output.emitWatermark(new Watermark(currentMaxTimestamp));
                            }

                            @Override
                            public void onPeriodicEmit(WatermarkOutput output) {

                            }
                        };
                    }
                }.withTimestampAssigner((tuple3, timestamp) -> tuple3.f0.getTime()));

        Table table = tableEnvironment.fromDataStream(rateHistory, $("rate_time").rowtime(), $("currency"), $("rate"));
        tableEnvironment.createTemporaryView("rates_history", table);
        TemporalTableFunction temporalTableFunction = table.createTemporalTableFunction($("rate_time"), $("currency"));
        tableEnvironment.createTemporarySystemFunction("rates", temporalTableFunction);

        Table result = tableEnvironment.sqlQuery(
                "SELECT\n" +
                        "  o.amount * r.rate AS amount\n" +
                        "FROM\n" +
                        "  orders AS o,\n" +
                        "  LATERAL TABLE (Rates(o.order_time)) AS r\n" +
                        "WHERE r.currency = o.currency"
        );
        tableEnvironment.toAppendStream(result, Row.class).print();
        environment.execute();
    }
}
