package com.lu.flink.table.join.versioned.table;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.Properties;

import static org.apache.flink.table.api.Expressions.$;

/**
 * +I(3,Euro,2,110,220,2020-12-04T01:10)
 * +I(4,Us Dollar,10,110,1100,2020-12-04T01:00)
 */
public class TemporalTableFunctionJoinDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        environment.disableOperatorChaining();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment, settings);

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9095");
        properties.put("group.id", "kafka-table-demo-group");
        FlinkKafkaConsumer<String> ratesHistoryConsumer = new FlinkKafkaConsumer<>("temporal-table-function-join-demo-rates-history", new SimpleStringSchema(), properties);
        ratesHistoryConsumer.setStartFromEarliest();
        DataStream<Tuple3<String, Integer, Timestamp>> ratesHistoryStream = environment.addSource(ratesHistoryConsumer)
                .map(message -> {
                    String[] split = message.split(",");
                    return Tuple3.of(split[0], Integer.valueOf(split[1]), Timestamp.valueOf(split[2]));
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT, Types.SQL_TIMESTAMP))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Integer, Timestamp>>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((tuple3, l) -> tuple3.f2.getTime()));
        Table ratesHistoryTable = tableEnvironment.fromDataStream(ratesHistoryStream, $("currency"), $("rate"), $("update_time").rowtime());
        tableEnvironment.createTemporaryView("rates_history", ratesHistoryTable);
        org.apache.flink.table.functions.TemporalTableFunction rates = ratesHistoryTable.createTemporalTableFunction($("update_time"), $("currency"));
        tableEnvironment.createTemporarySystemFunction("rates", rates);

        tableEnvironment.executeSql(
                "CREATE TABLE orders (" +
                        "   id INT," +
                        "   currency STRING," +
                        "   amount INT," +
                        "   order_time TIMESTAMP(3)," +
                        "   WATERMARK FOR order_time AS order_time" +
                        ") WITH (" +
                        "   'connector' = 'kafka'," +
                        "   'topic' = 'temporal-table-function-join-demo-orders'," +
                        "   'properties.bootstrap.servers' = 'localhost:9095'," +
                        "   'properties.group.id' = 'kafka-table-demo-group'," +
                        "   'format' = 'json'," +
                        "   'json.ignore-parse-errors' = 'true'," +
                        "   'scan.startup.mode' = 'earliest-offset'" +
                        ")"
        );

        tableEnvironment.executeSql(
                "CREATE TABLE print (id INT, currency STRING, amount INT, rate INT, order_result INT, order_time TIMESTAMP(3)) WITH ('connector' = 'print')"
        );

        tableEnvironment.executeSql(
                "INSERT INTO print" +
                        "   SELECT id, o.currency, amount, rate, amount * rate, order_time" +
                        "   FROM orders AS o," +
                        "   LATERAL TABLE (rates(o.order_time)) AS r" +
                        "   WHERE o.currency = r.currency"
        );

        environment.execute();
    }
}
