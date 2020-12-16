package com.lu.flink.table.join;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class EventTimeTemporalJoinDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(2);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment, settings);

        tableEnvironment.executeSql(
                "CREATE TABLE orders (" +
                        "    order_id    STRING," +
                        "    price       DECIMAL(32,2)," +
                        "    currency    STRING," +
                        "    order_time  TIMESTAMP(3)," +
                        "    WATERMARK FOR order_time AS order_time" +
                        ") WITH (" +
                        "   'connector' = 'kafka'," +
                        "   'topic' = 'event-time-temporal-join-demo-orders'," +
                        "   'properties.bootstrap.servers' = 'localhost:9095'," +
                        "   'properties.group.id' = 'kafka-table-demo-group'," +
                        "   'format' = 'csv'," +
                        "   'csv.field-delimiter' = ','," +
                        "   'scan.startup.mode' = 'earliest-offset'" +
                        ")");
        tableEnvironment.executeSql(
                "CREATE TABLE currency_rates (" +
                        "    currency STRING," +
                        "    conversion_rate DECIMAL(32, 2)," +
                        "    update_time TIMESTAMP(3)," +
                        "    WATERMARK FOR update_time AS update_time" +
                        ") WITH (" +
                        "   'connector' = 'kafka'," +
                        "   'topic' = 'event-time-temporal-join-demo-currency-rates'," +
                        "   'properties.bootstrap.servers' = 'localhost:9095'," +
                        "   'properties.group.id' = 'kafka-table-demo-group'," +
                        "   'format' = 'csv'," +
                        "   'csv.field-delimiter' = ','," +
                        "   'scan.startup.mode' = 'earliest-offset'" +
                        ")");

        // TODO Temporal Table Join requires primary key in versioned table, but no primary key can be found
        Table table = tableEnvironment.sqlQuery(
                "SELECT " +
                        "     order_id," +
                        "     price," +
                        "     orders.currency," +
                        "     conversion_rate," +
                        "     order_time" +
                        " FROM orders" +
                        " LEFT JOIN currency_rates FOR SYSTEM_TIME AS OF orders.order_time" +
                        " ON orders.currency = currency_rates.currency");
        tableEnvironment.toAppendStream(table, Row.class).print();
        environment.execute();
    }
}
