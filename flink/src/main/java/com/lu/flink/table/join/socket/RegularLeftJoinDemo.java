package com.lu.flink.table.join.socket;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * order_log:
 * <pre>
 * 1,1,2021-12-25 00:00:00
 * 2,2,2021-12-25 00:01:00
 * 3,3,2021-12-25 00:02:00
 * </pre>
 * <p>
 * price_log:
 * <pre>
 * 1,10,2021-12-25 00:00:00
 * 3,40,2021-12-25 00:02:00
 * </pre>
 */
public class RegularLeftJoinDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
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
                "   LEFT JOIN price_log p ON o.order_id = p.order_id");
    }
}
