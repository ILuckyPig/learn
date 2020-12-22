package com.lu.flink.table.query.configuration;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 8> +I(1,1)
 * 8> -U(1,1)
 * 8> +U(1,2)
 * wait 10 seconds
 * 8> +I(1,1)
 */
public class QueryConfigurationDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment, settings);

        TableConfig tableConfig = tableEnvironment.getConfig();
        tableConfig.setIdleStateRetention(Duration.ofSeconds(10));

        DataStream<String> source = environment.socketTextStream("localhost", 1111);
        tableEnvironment.createTemporaryView("clicks", source, $("id"));
        tableEnvironment.executeSql("CREATE TABLE print (id STRING, id_num BIGINT) WITH ('connector'='print')");

        tableEnvironment.executeSql("INSERT INTO print SELECT id,COUNT(*) FROM clicks GROUP BY id");

        environment.execute();
    }
}
