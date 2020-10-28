package com.lu.flink.datastream.operators.window.process.keyedprocess;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class KeyedProcessFunctionDemo {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(2);
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        environment.socketTextStream("localhost", 9999)
                .map(message -> {
                    String[] strings = message.split(",");
                    return Tuple3.of(strings[0], strings[1], LocalDateTime.parse(strings[2]));
                })
                .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.LOCAL_DATE_TIME))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Tuple3<String, String, LocalDateTime>>forMonotonousTimestamps()
                        .withTimestampAssigner((tuple3, timestamp) -> tuple3.f2.toInstant(ZoneOffset.of("+8")).toEpochMilli()))
                .keyBy(tuple2 -> tuple2.f0)
                .process(new CountWithTimeoutFunction())
                .print();
        environment.execute();
    }

    static class CountWithTimestamp {
        public String key;
        public long count;
        public long lastModified;

        @Override
        public String toString() {
            return "CountWithTimestamp{" +
                    "key='" + key + '\'' +
                    ", count=" + count +
                    ", lastModified=" + lastModified +
                    '}';
        }
    }

    static class CountWithTimeoutFunction extends KeyedProcessFunction<String, Tuple3<String, String, LocalDateTime>, Tuple2<String, Long>> {
        private ValueState<CountWithTimestamp> valueState;
        private final Logger log = LoggerFactory.getLogger(KeyedProcessFunctionDemo.class);

        @Override
        public void open(Configuration parameters) throws Exception {
            valueState = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", CountWithTimestamp.class));
        }

        @Override
        public void processElement(Tuple3<String, String, LocalDateTime> value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
            CountWithTimestamp current = valueState.value();
            if (current == null) {
                current = new CountWithTimestamp();
                current.key = value.f0;
            }
            current.count++;
            current.lastModified = ctx.timestamp();
            valueState.update(current);
            log.info("timestamp={}, {}", ctx.timestamp(), value);
            ctx.timerService().registerEventTimeTimer(current.lastModified + 60000);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Long>> out) throws Exception {
            CountWithTimestamp countWithTimestamp = valueState.value();
            if (countWithTimestamp.lastModified + 60000 == timestamp) {
                out.collect(Tuple2.of(countWithTimestamp.key, countWithTimestamp.count));
            } else {
                log.info("timestamp={}, {}", timestamp, countWithTimestamp);
            }
        }
    }
}
