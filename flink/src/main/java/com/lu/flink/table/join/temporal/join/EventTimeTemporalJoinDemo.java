package com.lu.flink.table.join.temporal.join;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * order_log:
 * <pre>
 * 1,1,2021-12-25 00:00:00
 * 2,2,2021-12-25 00:00:00
 * 3,3,2021-12-25 00:00:02
 * 4,4,2021-12-25 00:00:04
 * </pre>
 * <p>
 * price_log:
 * <pre>
 * INSERT,1,10,2021-12-25 00:00:01
 *
 * INSERT,2,20,2021-12-25 00:00:00
 *
 * INSERT,3,30,2021-12-25 00:00:00
 * DELETE,3,30,2021-12-25 00:00:00
 * INSERT,3,300,2021-12-25 00:00:02
 *
 * INSERT,4,40,2021-12-25 00:00:00
 * INSERT,4,400,2021-12-25 00:00:04
 * </pre>
 * <p>
 * result:
 * <pre>
 * +I[1, 1, null, 2021-12-25T00:00, null]
 * +I[2, 2, 20, 2021-12-25T00:00, 2021-12-25T00:00]
 * +I[3, 3, 300, 2021-12-25T00:00:02, 2021-12-25T00:00:02]
 * +I[4, 4, 400, 2021-12-25T00:00:04, 2021-12-25T00:00:04]
 * </pre>
 */
public class EventTimeTemporalJoinDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        tableEnvironment.executeSql("CREATE TABLE order_log (" +
                "   order_id INT" +
                "   ,movie_id INT" +
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
                "   ,price_timestamp TIMESTAMP(3)" +
                "   ,WATERMARK FOR price_timestamp AS price_timestamp" +
                "   ,PRIMARY KEY(order_id) NOT ENFORCED" +
                ") WITH (" +
                "   'connector'='socket'," +
                "   'hostname'='localhost'," +
                "   'port'='9998'," +
                "   'byte-delimiter'='10'," +
                "   'format'='changelog-csv'," +
                "   'changelog-csv.column-delimiter'=','" +
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
                "   FROM order_log o" +
                "   LEFT JOIN price_log FOR SYSTEM_TIME AS OF o.order_timestamp AS p" +
                "   ON o.order_id = p.order_id");
    }
}
