package com.lu.flink.table.join.socket;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class RegularJoinDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        tableEnvironment.executeSql("CREATE TABLE order_log (" +
                "   order_id INT" +
                "   ,movie_id INT" +
                "   ,user_id INT" +
                ") WITH (" +
                "   'connector'='socket'," +
                "   'hostname'='localhost'," +
                "   'port'='9999'," +
                "   'byte-delimiter'='10'," +
                "   'format'='csv'," +
                "   'csv.field-delimiter'=','" +
                ")");
        tableEnvironment.executeSql("CREATE TABLE print WITH ('connector'='print') LIKE order_log (EXCLUDING ALL)");
        tableEnvironment.executeSql("insert into print select * from order_log");
    }
}
