package com.lu.flink.table.join;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.Properties;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 时态表函数join
 */
public class EventTimeTemporalTableFunctionJoinDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.disableOperatorChaining();
        environment.setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment, settings);

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9095");
        properties.put("group.id", "kafka-table-demo-group");
        FlinkKafkaConsumer<String> ratesHistoryConsumer = new FlinkKafkaConsumer<>("test1", new SimpleStringSchema(), properties);

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
        TemporalTableFunction rates = ratesHistoryTable.createTemporalTableFunction($("update_time"), $("currency"));
        tableEnvironment.createTemporarySystemFunction("rates", rates);

        FlinkKafkaConsumer<String> orderConsumer = new FlinkKafkaConsumer<>("test2", new SimpleStringSchema(), properties);
        DataStream<Tuple4<Integer, Float, String, Timestamp>> orderStream = environment.addSource(orderConsumer)
                .map(message -> {
                    String[] split = message.split(",");
                    return Tuple4.of(Integer.valueOf(split[0]), Float.valueOf(split[1]), split[2], Timestamp.valueOf(split[3]));
                })
                .returns(Types.TUPLE(Types.INT, Types.FLOAT, Types.STRING, Types.SQL_TIMESTAMP))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple4<Integer, Float, String, Timestamp>>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((tuple4, l) -> tuple4.f3.getTime()));
        Table orderTable = tableEnvironment.fromDataStream(orderStream, $("id"), $("price"), $("currency"), $("order_time").rowtime());
        tableEnvironment.createTemporaryView("orders", orderTable);

        tableEnvironment.executeSql(
                "CREATE TABLE print (id INT, price FLOAT, currency STRING, rate INT, update_time TIMESTAMP(3)) WITH ('connector'='print')"
        );
        // TODO can not print any result
        tableEnvironment.executeSql(
                "INSERT INTO print" +
                        "  SELECT a.id,a.price,a.currency,r.rate,a.order_time FROM" +
                        "    orders AS a," +
                        "    LATERAL TABLE (rates(a.order_time)) AS r" +
                        "  WHERE a.currency = r.currency"
        );
        environment.execute();
    }
}
