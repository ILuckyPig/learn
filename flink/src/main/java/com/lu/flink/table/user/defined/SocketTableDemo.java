package com.lu.flink.table.user.defined;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class SocketTableDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment, settings);
        tableEnvironment.executeSql(
                "CREATE TABLE socket_source (" +
                        "   name STRING," +
                        "   score INT" +
                        ") WITH (" +
                        "   'connector'='socket'," +
                        "   'hostname'='localhost'," +
                        "   'port'='1111'," +
                        "   'byte-delimiter'='10'," +
                        "   'format'='changelog-csv'," +
                        "   'changelog-csv.column-delimiter'='|'" +
                        ")"
        );
        tableEnvironment.executeSql("SELECT name,SUM(score) FROM socket_source GROUP BY name").print();
    }
}
