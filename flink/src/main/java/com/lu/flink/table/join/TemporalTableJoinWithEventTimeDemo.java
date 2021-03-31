package com.lu.flink.table.join;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * https://developer.aliyun.com/article/679659?spm=a2c6h.13262185.0.0.3f777e18CP1xUo
 *
 * kafka-topics.sh --create --topic temporal-table-join-demo-order --bootstrap-server kafka_kafka_1:9092
 * kafka-topics.sh --create --topic temporal-table-join-demo-rates --bootstrap-server kafka_kafka_1:9092
 *
 * kafka-console-producer.sh --bootstrap-server kafka_kafka_1:9092 --topic temporal-table-join-demo-order
 * (1) Euro,10,2021-03-31 19:30:02
 * --> 保存到左表State中，注册Time(2021-03-31 19:30:02)
 * (2) Euro,10,2021-03-31 19:30:04
 * --> 保存到左表State中，注册Time(2021-03-31 19:30:04)
 *
 * kafka-console-producer.sh --bootstrap-server kafka_kafka_1:9092 --topic temporal-table-join-demo-rates
 * (3) { "op": "c", "before": null, "after": {"currency": "Euro", "rate": 114, "update_time": "2021-03-31 19:30:01"}, "source": { "version": "1.0.3.Final", "connector": "mysql", "name": "mysql-test-1", "query": null, "ts_ms": 1617190201000, "snapshot": "false", "db": "test", "table": "currency_rates", "server_id": 0, "gtid": null, "file": "mysql-bin.000007", "pos": 354, "row": 0, "thread": 4 }, "ts_ms": 1617190201000 }
 * --> 保存到右表State中，计算watermark为MIN( 2021-03-31 19:30:04, 2021-03-31 19:30:01 )=>2021-03-31 19:30:01，
 * 此时watermark没有越过左表任意一个Time触发器时间，所以不会触发计算
 * (4) { "op": "u", "before": {"currency": "Euro", "rate": 114, "update_time": "2021-03-31 19:30:01"}, "after": {"currency": "Euro", "rate": 116, "update_time": "2021-03-31 19:30:03"}, "source": { "version": "1.0.3.Final", "connector": "mysql", "name": "mysql-test-1", "query": null, "ts_ms": 1617190203000, "snapshot": "false", "db": "test", "table": "currency_rates", "server_id": 0, "gtid": null, "file": "mysql-bin.000007", "pos": 354, "row": 0, "thread": 4 }, "ts_ms": 1617190203000 }
 * --> 保存到右表State中，计算watermark为MIN( 2021-03-31 19:30:01, 2021-03-31 19:30:03 )=>2021-03-31 19:30:03，
 * 此时watermark越过了左表Time(2021-03-31 19:30:02)，触发计算
 * (5) { "op": "u", "before": {"currency": "Euro", "rate": 116, "update_time": "2021-03-31 19:30:03"}, "after": {"currency": "Euro", "rate": 119, "update_time": "2021-03-31 19:30:05"}, "source": { "version": "1.0.3.Final", "connector": "mysql", "name": "mysql-test-1", "query": null, "ts_ms": 1617190205000, "snapshot": "false", "db": "test", "table": "currency_rates", "server_id": 0, "gtid": null, "file": "mysql-bin.000007", "pos": 354, "row": 0, "thread": 4 }, "ts_ms": 1617190205000 }
 *
 * > output
 * +I(2021-03-31T19:30:02,Euro,10,1140)
 * +I(2021-03-31T19:30:04,Euro,10,1160)
 *
 */
public class TemporalTableJoinWithEventTimeDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.disableOperatorChaining();
        environment.setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment, settings);

        tableEnvironment.executeSql(
                "CREATE TABLE orders (" +
                        "   currency STRING," +
                        "   amount INT," +
                        "   order_time TIMESTAMP(3)," +
                        "   WATERMARK FOR order_time AS order_time" +
                        ") WITH (" +
                        "   'connector' = 'kafka'," +
                        "   'topic' = 'temporal-table-join-demo-order'," +
                        "   'properties.bootstrap.servers' = 'localhost:9095'," +
                        "   'properties.group.id' = 'kafka-table-demo-group'," +
                        "   'format' = 'csv'," +
                        "   'scan.startup.mode' = 'earliest-offset'" +
                        ")"
        );

        tableEnvironment.executeSql(
                "CREATE TABLE rates (" +
                        "   currency STRING," +
                        "   rate INT," +
                        "   update_time TIMESTAMP(3) METADATA FROM 'value.source.timestamp' VIRTUAL," +
                        "   PRIMARY KEY (currency) NOT ENFORCED," +
                        "   WATERMARK FOR update_time AS update_time" +
                        ") WITH (" +
                        "   'connector' = 'kafka'," +
                        "   'topic' = 'temporal-table-join-demo-rates'," +
                        "   'properties.bootstrap.servers' = 'localhost:9095'," +
                        "   'properties.group.id' = 'kafka-table-demo-group'," +
                        "   'format' = 'debezium-json'," +
                        "   'scan.startup.mode' = 'earliest-offset'" +
                        ")"
        );

        tableEnvironment.executeSql(
                "CREATE TABLE print (" +
                        "   ts TIMESTAMP(3)," +
                        "   currency STRING," +
                        "   amount INT," +
                        "   yen_amount INT" +
                        ") WITH (" +
                        "   'connector'='print'" +
                        ")"
        );

        tableEnvironment.executeSql(
                "INSERT INTO print" +
                        "   SELECT o.order_time,o.currency,o.amount,o.amount * r.rate as yen_amount" +
                        "   FROM orders as o" +
                        "   JOIN rates FOR SYSTEM_TIME AS OF o.order_time AS r" +
                        "   ON o.currency = r.currency"
        );
    }
}
