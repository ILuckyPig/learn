package com.lu.flink.table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.KafkaValidator;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Convert a Table into DataStream
 * Retract Mode
 */
public class KafkaTableDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
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
                        .field("id", DataTypes.INT())
                        .field("word", DataTypes.STRING())
                )
                .inAppendMode()
                .createTemporaryTable("search_log");
        Table searchLog = tableEnvironment.from("search_log");
        Table result = searchLog
                .groupBy($("id"), $("word"))
                .select($("id"), $("word"), $("date").count());
        DataStream<Tuple2<Boolean, Row>> stream = tableEnvironment.toRetractStream(result, Row.class);
        stream.print();
        environment.execute();
    }
}
