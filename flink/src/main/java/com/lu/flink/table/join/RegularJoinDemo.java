package com.lu.flink.table.join;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * EvenTime不能用于regular join
 *
 * 1> 2020-12-04T19:14:09,50,Yen,2020-12-04T09:00,Yen,1
 * 1> 2020-12-04T10:52,5,Us Dollar,2020-12-04T09:00,Us Dollar,102
 * 1> 2020-12-04T10:15,2,Euro,2020-12-04T11:15,Euro,119
 * 1> 2020-12-04T10:32,3,Euro,2020-12-04T11:15,Euro,119
 * 1> 2020-12-04T10:15,2,Euro,2020-12-04T09:00,Euro,114
 * 1> 2020-12-04T10:32,3,Euro,2020-12-04T09:00,Euro,114
 * 1> 2020-12-04T10:15,2,Euro,2020-12-04T10:45,Euro,116
 * 1> 2020-12-04T10:32,3,Euro,2020-12-04T10:45,Euro,116
 */
public class RegularJoinDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        // environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        environment.enableCheckpointing(1000);
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
                        // "   WATERMARK FOR order_time AS order_time" +
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
                        // "   WATERMARK FOR history_time AS history_time" +
                        ") WITH (" +
                        "   'connector' = 'kafka'," +
                        "   'topic' = 'RatesHistory'," +
                        "   'properties.bootstrap.servers' = '127.0.0.1:9092'," +
                        "   'properties.group.id' = 'kafka-table-demo-group'," +
                        "   'format' = 'csv'," +
                        "   'csv.field-delimiter' = ','," +
                        "   'scan.startup.mode' = 'earliest-offset'" +
                        ")");
        Table table = tableEnvironment.sqlQuery("SELECT * FROM orders INNER JOIN rates_history ON orders.currency = rates_history.currency");
        tableEnvironment.toAppendStream(table, Row.class).print();
        environment.execute();
    }
}
