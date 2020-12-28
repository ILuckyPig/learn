package com.lu.flink.table.sql;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.sql.Timestamp;
import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

public class SqlDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(2);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment, settings);

        DataStream<Tuple3<Integer, Integer, Timestamp>> source = environment.socketTextStream("localhost", 1111)
                .map(message -> {
                    String[] split = message.split(",");
                    return Tuple3.of(Integer.parseInt(split[0]), Integer.parseInt(split[1]), Timestamp.valueOf(split[2]));
                })
                .returns(Types.TUPLE(Types.INT, Types.INT, Types.SQL_TIMESTAMP))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<Integer, Integer, Timestamp>>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner((tuple3, l) -> tuple3.f2.getTime()));

        tableEnvironment.createTemporaryView("source", source, $("id"), $("amount"), $("order_time").rowtime());
        tableEnvironment.executeSql("CREATE TABLE print (id INT, amount INT) WITH ('connector'='print')");
        tableEnvironment.executeSql(
                "INSERT INTO print SELECT id,SUM(amount) FROM source GROUP BY TUMBLE(order_time, INTERVAL '10' SECONDS),id"
        );
        tableEnvironment.execute("");
    }
}
