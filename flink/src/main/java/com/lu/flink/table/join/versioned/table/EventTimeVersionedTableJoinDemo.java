package com.lu.flink.table.join.versioned.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * +I(1,2020-12-04T09:00,Euro,null,null)
 * +I(2,2020-12-04T09:00,Euro,null,null)
 * +I(3,2020-12-04T09:00:10,Euro,null,null)
 * +I(5,2020-12-04T09:10,Euro,2020-12-04T09:10,114.0)
 * +I(6,2020-12-04T09:45,Euro,2020-12-04T09:10,114.0)
 * +I(7,2020-12-04T10:45,Euro,2020-12-04T10:45,116.0)
 * +I(4,2020-12-04T09:00,Us Dollar,2020-12-04T09:00,102.0)
 * +I(8,2020-12-04T11:01:10,Yen,2020-12-04T11:01:10,1.0)
 */
public class EventTimeVersionedTableJoinDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.disableOperatorChaining();
        environment.setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment, settings);

        tableEnvironment.executeSql(
                "CREATE TABLE currency_rates (" +
                        "   currency STRING," +
                        "   conversion_rate DECIMAL(32, 2)," +
                        "   update_time TIMESTAMP(3) METADATA FROM 'value.source.timestamp' VIRTUAL," +
                        "   PRIMARY KEY (currency) NOT ENFORCED," +
                        "   WATERMARK FOR update_time AS update_time" +
                        ") WITH (" +
                        "   'connector' = 'kafka'," +
                        "   'topic' = 'event-time-versioned-table-join-demo-currency-rates'," +
                        "   'properties.bootstrap.servers' = 'localhost:9095'," +
                        "   'properties.group.id' = 'kafka-table-demo-group'," +
                        "   'format' = 'debezium-json'," +
                        "   'scan.startup.mode' = 'earliest-offset'" +
                        ")"
        );

        tableEnvironment.executeSql(
                "CREATE TABLE orders (" +
                        "   order_id INT," +
                        "   currency STRING," +
                        "   order_time TIMESTAMP(3)," +
                        "   WATERMARK FOR order_time AS order_time" +
                        ") WITH (" +
                        "   'connector' = 'kafka'," +
                        "   'topic' = 'event-time-versioned-table-join-demo-orders'," +
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
                        "  LEFT JOIN currency_rates FOR SYSTEM_TIME AS OF o.order_time AS p" +
                        "  ON o.currency = p.currency"
        );
        environment.execute();
    }
}
