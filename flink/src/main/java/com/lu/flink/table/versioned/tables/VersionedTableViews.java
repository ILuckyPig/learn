package com.lu.flink.table.versioned.tables;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class VersionedTableViews {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(2);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment, settings);
        tableEnvironment.executeSql(
                "CREATE TABLE currency_rates (" +
                        "    currency STRING," +
                        "    conversion_rate DECIMAL(32, 2)," +
                        "    update_time TIMESTAMP(3)," +
                        "    WATERMARK FOR update_time AS update_time" +
                        ") WITH (" +
                        "   'connector' = 'kafka'," +
                        "   'topic' = 'versioned-table-views-currency-rates'," +
                        "   'properties.bootstrap.servers' = 'localhost:9095'," +
                        "   'properties.group.id' = 'kafka-table-demo-group'," +
                        "   'format' = 'json'," +
                        "   'scan.startup.mode' = 'earliest-offset'" +
                        ")"
        );
        tableEnvironment.executeSql(
                "CREATE VIEW versioned_rates AS" +
                        "   SELECT currency, conversion_rate, update_time" +
                        "   FROM (" +
                        "       SELECT *," +
                        "       ROW_NUMBER() OVER (PARTITION BY currency ORDER BY update_time DESC) AS rownum" +
                        "       FROM currency_rates)" +
                        "   WHERE rownum = 1"
        );
        tableEnvironment.executeSql("CREATE TABLE print_table WITH ('connector'='print') LIKE currency_rates (EXCLUDING ALL)");
        tableEnvironment.executeSql("INSERT INTO print_table SELECT * FROM versioned_rates");
        environment.execute();
    }
}
