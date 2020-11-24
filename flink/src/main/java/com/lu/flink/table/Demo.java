package com.lu.flink.table;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

public class Demo {
    public static void main(String[] args) throws Exception {
        // TODO Expected LocalReferenceExpression. Got: date
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment, settings);

        tableEnvironment
                .connect(new Kafka()
                        .version("universal")
                        .topic("user-search-log")
                        .property("bootstrap.servers", "127.0.0.1:9092")
                        .property(KafkaValidator.CONNECTOR_PROPERTIES_GROUP_ID, "kafka-table-demo-group")
                        .startFromEarliest()
                )
                .withFormat(new Csv().fieldDelimiter(','))
                .withSchema(new Schema()
                        .field("date", DataTypes.TIMESTAMP(3))
                        .rowtime(new Rowtime().timestampsFromField("date").watermarksFromSource())
                        .field("id", DataTypes.INT())
                        .field("word", DataTypes.STRING())
                )
                .inAppendMode()
                .createTemporaryTable("search_log");
        Table searchLog = tableEnvironment.from("search_log");
        Table result = searchLog
                .window(Tumble
                        .over(lit(10).seconds())
                        .on($("date").rowtime())
                        .as("w")
                )
                .groupBy($("id"), $("w"))
                .select($("id"), $("w").end(), $("word").count());
        DataStream<Row> stream = tableEnvironment.toAppendStream(result, Row.class);
        stream.print();
        environment.execute();
    }
}
