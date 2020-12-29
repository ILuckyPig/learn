package com.lu.flink.datastream.batch;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.math.BigInteger;
import java.util.HashSet;
import java.util.Set;

public class BatchModeDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setRuntimeMode(RuntimeExecutionMode.BATCH);
        Row row1 = Row.of(BigInteger.valueOf(1L), "A");
        Row row2 = Row.of(BigInteger.valueOf(2L), "A");
        Row row3 = Row.of(BigInteger.valueOf(3L), "B");
        Row row4 = Row.of(BigInteger.valueOf(4L), "C");
        Row row5 = Row.of(BigInteger.valueOf(5L), "D");

        DataStreamSource<Row> rowDataStreamSource = environment.fromElements(row1, row2, row3, row4, row5);
        // TODO jdbc bounded source
        rowDataStreamSource
        // List<Integer> partitions = Stream.iterate(0, i -> i + 1).limit(128).collect(Collectors.toList());
        // environment.createInput(JdbcInputFormat.buildJdbcInputFormat()
        //         .setDrivername("com.mysql.cj.jdbc.Driver")
        //         .setDBUrl("jdbc:mysql://localhost:3306/test?serverTimezone=Asia/Shanghai")
        //         .setUsername("root")
        //         .setPassword("root")
        //         .setQuery("SELECT id,name FROM weibo_user PARTITION(p?) WHERE followers_count > 1000")
        //         .setRowTypeInfo(new RowTypeInfo(
        //                 Types.BIG_INT,
        //                 Types.STRING
        //         ))
        //         .setParametersProvider(new JdbcParameterValuesProvider() {
        //             @Override
        //             public Serializable[][] getParameterValues() {
        //                 Serializable[][] serializable = new Serializable[partitions.size()][1];
        //                 for (int i = 0; i < partitions.size(); i++) {
        //                     serializable[i] = new Serializable[]{partitions.get(i)};
        //                 }
        //                 return serializable;
        //             }
        //         })
        //         .finish())
                .keyBy(row -> (String) row.getField(1))
                .process(new KeyedProcessFunction<String, Row, Row>() {
                    Set<BigInteger> set;
                    Row row;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        set = new HashSet<>();
                        row = new Row(3);
                    }

                    @Override
                    public void processElement(Row value, Context ctx, Collector<Row> out) throws Exception {
                        set.add((BigInteger) value.getField(0));
                        ctx.timerService().registerProcessingTimeTimer(Long.MAX_VALUE);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Row> out) throws Exception {
                        row.setField(0, ctx.getCurrentKey());
                        row.setField(1, set.size());
                        row.setField(2, set);
                        out.collect(row);
                        set.clear();
                    }
                })
                .print();

        environment.execute();
    }
}
