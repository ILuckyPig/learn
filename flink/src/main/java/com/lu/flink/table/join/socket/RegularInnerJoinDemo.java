package com.lu.flink.table.join.socket;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class RegularInnerJoinDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        tableEnvironment.executeSql("CREATE TABLE order_log (" +
                "   order_id INT" +
                "   ,movie_id INT" +
                "   ,order_timestamp TIMESTAMP" +
                ") WITH (" +
                "   'connector'='socket'," +
                "   'hostname'='localhost'," +
                "   'port'='9999'," +
                "   'byte-delimiter'='10'," +
                "   'format'='csv'," +
                "   'csv.field-delimiter'=','" +
                ")");
        tableEnvironment.executeSql("CREATE TABLE price_log (" +
                "   order_id INT" +
                "   ,seat_price INT" +
                "   ,price_timestamp TIMESTAMP" +
                ") WITH (" +
                "   'connector'='socket'," +
                "   'hostname'='localhost'," +
                "   'port'='9998'," +
                "   'byte-delimiter'='10'," +
                "   'format'='csv'," +
                "   'csv.field-delimiter'=','" +
                ")");
        tableEnvironment.executeSql("CREATE TABLE print (" +
                "   order_id INT" +
                "   ,movie_id INT" +
                "   ,seat_price INT" +
                "   ,order_timestamp TIMESTAMP" +
                ") WITH (" +
                "   'connector'='print'" +
                ")");
        tableEnvironment.executeSql("INSERT INTO print" +
                "   SELECT" +
                "       o.order_id" +
                "       ,movie_id" +
                "       ,seat_price" +
                "       ,order_timestamp" +
                "   FROM order_log o" +
                "   INNER JOIN price_log p ON o.order_id = p.order_id");
    }
}
