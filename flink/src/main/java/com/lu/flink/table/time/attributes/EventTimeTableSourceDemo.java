package com.lu.flink.table.time.attributes;

import org.apache.commons.net.ntp.TimeStamp;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.PreserveWatermarks;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class EventTimeTableSourceDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        environment.setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment, settings);

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "127.0.0.1:9092");
        properties.put("group.id", "kafka-table-demo-group");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("user-search-log", new SimpleStringSchema(), properties);
        consumer.setStartFromEarliest();

        class UserSearchLogSource implements StreamTableSource<Row>, DefinedRowtimeAttributes {
            @Override
            public DataType getProducedDataType() {
                // TODO data type
                return DataTypes.ROW(
                        DataTypes.FIELD("log_time", DataTypes.TIMESTAMP(3)),
                        DataTypes.FIELD("id", DataTypes.INT()),
                        DataTypes.FIELD("word", DataTypes.STRING())
                );
            }

            @Override
            public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
                RowtimeAttributeDescriptor descriptor = new RowtimeAttributeDescriptor("log_time",
                        new ExistingField("log_time"), new PreserveWatermarks());
                return Collections.singletonList(descriptor);
            }

            @Override
            public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
                DataStream<String> source = environment.addSource(consumer);
                return source
                        .map(new MapFunction<String, Row>() {
                            @Override
                            public Row map(String value) throws Exception {
                                String[] strings = value.split(",");
                                Row row = new Row(3);
                                row.setField(0, Timestamp.valueOf(strings[0]));
                                row.setField(1, Integer.parseInt(strings[1]));
                                row.setField(2, strings[2]);
                                return row;
                            }
                        })
                        .assignTimestampsAndWatermarks(new WatermarkStrategy<Row>() {
                            @Override
                            public WatermarkGenerator<Row> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                                return new WatermarkGenerator<Row>() {
                                    private long currentMaxTimestamp = Long.MIN_VALUE;

                                    @Override
                                    public void onEvent(Row event, long eventTimestamp, WatermarkOutput output) {
                                        currentMaxTimestamp = Math.max(((TimeStamp) event.getField(0)).getTime(), currentMaxTimestamp);
                                        output.emitWatermark(new Watermark(currentMaxTimestamp));
                                    }

                                    @Override
                                    public void onPeriodicEmit(WatermarkOutput output) {

                                    }
                                };
                            }
                        }.withTimestampAssigner((event, timestamp) -> ((TimeStamp) event.getField(0)).getTime()));
            }

            @Override
            public TableSchema getTableSchema() {
                TableSchema tableSchema = TableSchema.builder()
                        .field("log_time", DataTypes.TIMESTAMP(3))
                        .field("id", DataTypes.INT())
                        .field("word", DataTypes.STRING())
                        .build();
                return tableSchema;
            }
        }

        tableEnvironment.registerTableSource("user_search_log", new UserSearchLogSource());

        Table table = tableEnvironment.sqlQuery(
                "SELECT " +
                        "   id," +
                        "   TUMBLE_END(log_time,INTERVAL '10' SECONDS) AS end_time," +
                        "   COUNT(word) AS cnt" +
                        " FROM user_search_log" +
                        " GROUP BY" +
                        "   id," +
                        "   TUMBLE(log_time,INTERVAL '10' SECONDS)");

        tableEnvironment.toAppendStream(table, Row.class).print();

        environment.execute();
    }
}
