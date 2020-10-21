package com.lu.flink.operators.window.incremental.reduce;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.List;

public class IncrementalReduceDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        environment.socketTextStream("localhost", 9999)
                .map(message -> {
                    String[] strings = message.split(",");
                    return Tuple3.of(strings[0], Long.parseLong(strings[1]), Integer.parseInt(strings[2]));
                })
                .returns(Types.TUPLE(Types.STRING, Types.LONG, Types.INT))
                .keyBy(tuple2 -> tuple2.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
                .reduce((t1, t2) -> (t1.f2 > t2.f2 ? t1 : t2), new MyProcessWindowFunction())
                .print();

        environment.execute();
    }

    public static class MyProcessWindowFunction extends ProcessWindowFunction<Tuple3<String, Long, Integer>, Tuple3<String, Long, Tuple3<String, Long, Integer>>, String, TimeWindow> {
        @Override
        public void process(String key,
                            Context context,
                            Iterable<Tuple3<String, Long, Integer>> elements,
                            Collector<Tuple3<String, Long, Tuple3<String, Long, Integer>>> out) throws Exception {
            Tuple3<String, Long, Integer> tuple3 = null;
            List<Tuple3<String, Long, Integer>> list = Lists.newArrayList(elements);
            System.out.println(list.size());
            for (Tuple3<String, Long, Integer> element : list) {
                tuple3 = element;
                System.out.println(context.window().toString() + ", " + element);
            }
            out.collect(Tuple3.of(key, context.window().getEnd(), tuple3));
        }
    }
}
