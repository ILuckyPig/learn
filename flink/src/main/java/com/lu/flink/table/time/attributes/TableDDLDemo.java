package com.lu.flink.table.time.attributes;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TableDDLDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(2);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment, settings);
        tableEnvironment.executeSql(
                "CREATE TABLE user_search_log (" +
                        "   log_time TIMESTAMP(3)," +
                        "   id INT," +
                        "   word STRING," +
                        "   WATERMARK FOR log_time AS log_time" +
                        ") WITH (" +
                        "   'connector' = 'kafka'," +
                        "   'topic' = 'user-search-log'," +
                        "   'properties.bootstrap.servers' = '127.0.0.1:9092'," +
                        "   'properties.group.id' = 'kafka-table-demo-group'," +
                        "   'format' = 'csv'," +
                        "   'csv.field-delimiter' = ','," +
                        "   'scan.startup.mode' = 'earliest-offset'" +
                        ")");

        TableResult tableResult = tableEnvironment.executeSql(
                "SELECT " +
                        "   id," +
                        "   TUMBLE_END(log_time,INTERVAL '10' SECONDS) AS end_time," +
                        "   COUNT(word) AS cnt" +
                        " FROM user_search_log" +
                        " GROUP BY" +
                        "   id," +
                        "   TUMBLE(log_time,INTERVAL '10' SECONDS)");
        // TODO print
        tableEnvironment.execute("table.ddl.event.time.demo");
    }
}
