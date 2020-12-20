package com.lu.flink.table.join.versioned.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * +I(3,2020-12-04T09:00,Us Dollar,2020-12-20T13:14:45,100.0)
 */
public class ProcessTimeCommentTableJoinDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.disableOperatorChaining();
        environment.setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment, settings);

        tableEnvironment.executeSql(
                "CREATE TABLE lates_rates (" +
                        "   currency VARCHAR(100)," +
                        "   conversion_rate FLOAT," +
                        "   update_time TIMESTAMP(3)," +
                        "   PRIMARY KEY(currency) NOT ENFORCED" +
                        ") WITH (" +
                        "   'connector'='jdbc'," +
                        "   'url'='jdbc:mysql://localhost:3306/learn?serverTimezone=GMT%2B8'," +
                        "   'driver'='com.mysql.cj.jdbc.Driver'," +
                        "   'username'='root'," +
                        "   'password'='root'," +
                        "   'table-name'='lates_rates'" +
                        ")"
        );

        tableEnvironment.executeSql(
                "CREATE TABLE orders (" +
                        "   order_id INT," +
                        "   currency STRING," +
                        "   order_time TIMESTAMP(3)," +
                        "   proctime AS PROCTIME()" +
                        ") WITH (" +
                        "   'connector' = 'kafka'," +
                        "   'topic' = 'process-time-comment-table-join-demo-orders'," +
                        "   'properties.bootstrap.servers' = 'localhost:9095'," +
                        "   'properties.group.id' = 'kafka-table-demo-group'," +
                        "   'format' = 'json'," +
                        "   'json.ignore-parse-errors'='true'," +
                        "   'scan.startup.mode' = 'earliest-offset'" +
                        ")"
        );

        tableEnvironment.executeSql(
                "CREATE TABLE print (order_id INT, order_time TIMESTAMP(3), currency STRING, update_time TIMESTAMP(3), conversion_rate FLOAT) WITH ('connector'='print')"
        );
        tableEnvironment.executeSql(
                "INSERT INTO print" +
                        "  SELECT order_id,order_time,o.currency,update_time,conversion_rate FROM" +
                        "  orders AS o" +
                        "  JOIN lates_rates FOR SYSTEM_TIME AS OF o.proctime AS p" +
                        "  ON o.currency = p.currency"
        );
        environment.execute();
    }
}
