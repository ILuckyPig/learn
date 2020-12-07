package com.lu.flink.table.join;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class JoinTemporalTableDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        environment.enableCheckpointing(10 * 1000);
        environment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        environment.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        environment.setParallelism(2);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment, settings);

        tableEnvironment.executeSql(
                "CREATE TABLE orders (" +
                        "   order_time TIMESTAMP(3)," +
                        "   amount INT," +
                        "   currency STRING," +
                        "   process_time AS PROCTIME()" +
                        ") WITH (" +
                        "   'connector' = 'kafka'," +
                        "   'topic' = 'Orders'," +
                        "   'properties.bootstrap.servers' = '127.0.0.1:9092'," +
                        "   'properties.group.id' = 'kafka-table-demo-group'," +
                        "   'format' = 'csv'," +
                        "   'csv.field-delimiter' = ','," +
                        "   'scan.startup.mode' = 'earliest-offset'" +
                        ")");
        tableEnvironment.executeSql(
                "CREATE TABLE rates_history (" +
                        "   history_time TIMESTAMP(3)," +
                        "   currency STRING," +
                        "   rate INT" +
                        ") WITH (" +
                        "   'connector' = 'kafka'," +
                        "   'topic' = 'RatesHistory'," +
                        "   'properties.bootstrap.servers' = '127.0.0.1:9092'," +
                        "   'properties.group.id' = 'kafka-table-demo-group'," +
                        "   'format' = 'csv'," +
                        "   'csv.field-delimiter' = ','," +
                        "   'scan.startup.mode' = 'earliest-offset'" +
                        ")");
        // TODO sql error
        Table table = tableEnvironment.sqlQuery(
                "SELECT\n" +
                        "  o.amount, o.currency, r.rate, o.amount * r.rate\n" +
                        "FROM\n" +
                        "  orders AS o\n" +
                        "  JOIN rates_history FOR SYSTEM_TIME AS OF o.process_time AS r\n" +
                        "  ON r.currency = o.currency"
        );
        tableEnvironment.toAppendStream(table, Row.class).print();
        environment.execute();
    }
}
