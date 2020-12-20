package com.lu.flink.table.versioned.tables;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class CommonTable {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(2);
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
                "CREATE TABLE print WITH ('connector'='print') LIKE lates_rates (EXCLUDING ALL)"
        );
        tableEnvironment.executeSql(
                "INSERT INTO print SELECT * FROM lates_rates"
        );
        environment.execute();
    }
}
