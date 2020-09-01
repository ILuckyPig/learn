package com.lu.flink.process.function;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class ProcessFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        environment.setParallelism(3);

        // 1.11 generate water
        WatermarkStrategy<Tuple4<Integer, LocalDateTime, Long, Float>> strategy = WatermarkStrategy
                // 指定延时时间
                .<Tuple4<Integer, LocalDateTime, Long, Float>>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                // 指定event time
                .withTimestampAssigner((tuple4, timestamp) -> tuple4.f2);

        DataStream<Tuple4<Integer, LocalDateTime, Long, Float>> source = environment
                .socketTextStream("localhost", 9999)
                .map(message -> {
                    String[] strings = message.split(",");
                    int key = Integer.parseInt(strings[0]);
                    LocalDateTime time = LocalDateTime.parse(strings[1]);
                    long timestamp = time.toInstant(ZoneOffset.of("+8")).toEpochMilli();
                    float tip = Float.parseFloat(strings[2]);
                    return Tuple4.of(key, time, timestamp, tip);
                })
                .returns(Types.TUPLE(Types.INT, Types.LOCAL_DATE_TIME, Types.LONG, Types.FLOAT))
                .assignTimestampsAndWatermarks(strategy);

        source
                .keyBy(tuple4 -> tuple4.f0)
                .process(new PseudoWindow(Time.seconds(10)))
                .print();

        environment.execute();
    }

    static class PseudoWindow extends KeyedProcessFunction<Integer, Tuple4<Integer, LocalDateTime, Long, Float>, Tuple3<Integer, Long, Float>> {
        private final long durationMsec;
        private transient MapState<Long, Float> sumOfTips;

        PseudoWindow(Time duration) {
            this.durationMsec = duration.toMilliseconds();
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<Long, Float> sumDesc = new MapStateDescriptor<>("sumOfTips", Long.class, Float.class);
            sumOfTips = getRuntimeContext().getMapState(sumDesc);
        }

        @Override
        public void processElement(Tuple4<Integer, LocalDateTime, Long, Float> value,
                                   Context ctx,
                                   Collector<Tuple3<Integer, Long, Float>> out) throws Exception {
            Long eventTime = value.f2;
            TimerService timerService = ctx.timerService();

            if (eventTime <= timerService.currentWatermark()) {
                // this event time is late;
                // ctx.output(new OutputTag<Tuple4<Integer, LocalDateTime, Long, Float>>("id"){}, value);
            } else {
                long endOfWindow = (eventTime - (eventTime % durationMsec) + durationMsec - 1);

                timerService.registerEventTimeTimer(endOfWindow);

                Float sum = sumOfTips.get(endOfWindow);
                if (null == sum) {
                    sum = 0.0f;
                }
                sum += value.f3;
                sumOfTips.put(endOfWindow, sum);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple3<Integer, Long, Float>> out) throws Exception {
            Integer currentKey = ctx.getCurrentKey();
            Float sumOfTip = sumOfTips.get(timestamp);
            out.collect(Tuple3.of(currentKey, timestamp, sumOfTip));
            sumOfTips.remove(timestamp);
        }
    }
}
