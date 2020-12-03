package com.lu.flink.table.join;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

// TODO IntervalJoinDemo
public class IntervalJoinDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
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
                        "   currency STRING" +
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
        tableEnvironment.executeSql(
                "CREATE TABLE rates_history (" +
                        "   history_time TIMESTAMP(3)," +
                        "   currency STRING," +
                        "   rate INT" +
                        "   WATERMARK FOR history_time AS history_time" +
                        ") WITH (" +
                        "   'connector' = 'kafka'," +
                        "   'topic' = 'RatesHistory'," +
                        "   'properties.bootstrap.servers' = '127.0.0.1:9092'," +
                        "   'properties.group.id' = 'kafka-table-demo-group'," +
                        "   'format' = 'csv'," +
                        "   'csv.field-delimiter' = ','," +
                        "   'scan.startup.mode' = 'earliest-offset'" +
                        ")");

        Table table = tableEnvironment.sqlQuery("SELECT * FROM orders, rate_history WHERE orders.currency = rates_history.currency" +
                " AND orders.order_time BETWEEN ");
        tableEnvironment.toAppendStream(table, Row.class).print();
        environment.execute();
    }
}
