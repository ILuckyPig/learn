package com.lu.flink.table.join.interval.join;

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
 * 1,10,2021-12-24 23:59:59
 * 2,20,2021-12-25 00:01:10
 * 3,40,2021-12-25 00:02:10
 * </pre>
 */
public class IntervalRightJoinDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        tableEnvironment.executeSql("CREATE TABLE order_log (" +
                "   order_id INT" +
                "   ,movie_id INT" +
//                "   ,order_timestamp AS PROCTIME()" +
                "   ,order_timestamp TIMESTAMP(3)" +
                "   ,WATERMARK FOR order_timestamp AS order_timestamp" +
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
//                "   ,price_timestamp AS PROCTIME()" +
                "   ,price_timestamp TIMESTAMP(3)" +
                "   ,WATERMARK FOR price_timestamp AS price_timestamp" +
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
                "   ,price_timestamp TIMESTAMP" +
                ") WITH (" +
                "   'connector'='print'" +
                ")");
        tableEnvironment.executeSql("INSERT INTO print" +
                "   SELECT" +
                "       o.order_id" +
                "       ,movie_id" +
                "       ,seat_price" +
                "       ,order_timestamp" +
                "       ,price_timestamp" +
                "   FROM price_log p" +
                "   RIGHT JOIN order_log o" +
                "   ON o.order_id = p.order_id" +
                "   AND p.price_timestamp BETWEEN o.order_timestamp - INTERVAL '1' SECOND AND o.order_timestamp + INTERVAL '10' SECOND");
    }
}
