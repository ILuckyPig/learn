package com.lu.flink.sql;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class TopDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(2);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment, settings);

        DataStream<Tuple4<Integer, Integer, String, Long>> source = environment.socketTextStream("localhost", 1111)
                .map(message -> {
                    String[] split = message.split(",");
                    return Tuple4.of(Integer.parseInt(split[0]), Integer.parseInt(split[1]), split[2], Long.parseLong(split[3]));
                })
                .returns(Types.TUPLE(Types.INT, Types.INT, Types.STRING, Types.LONG));
        tableEnvironment.createTemporaryView("source", source, $("id"), $("category"), $("product_name"), $("sales"));
        // have rank num
        // tableEnvironment.executeSql(
        //         "SELECT * FROM" +
        //                 "   (" +
        //                 "       SELECT *,ROW_NUMBER() OVER (PARTITION BY category ORDER BY sales DESC) as row_num" +
        //                 "       FROM source" +
        //                 "   )" +
        //                 "   WHERE row_num <= 5"
        // ).print();
        tableEnvironment.executeSql(
                "SELECT id,category,product_name,sales FROM" +
                        "   (" +
                        "       SELECT *,ROW_NUMBER() OVER (PARTITION BY category ORDER BY sales DESC) as row_num" +
                        "       FROM source" +
                        "   )" +
                        "   WHERE row_num <= 5"
        ).print();
    }
}
