package com.lu.flink.operators.window.process.window.function;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MyWasteFulMax extends ProcessWindowFunction<Tuple3<String, Long, Integer>, Tuple3<String, Long, Integer>, String, TimeWindow> {
    @Override
    public void process(String key,
                        Context context,
                        Iterable<Tuple3<String, Long, Integer>> elements,
                        Collector<Tuple3<String, Long, Integer>> out) throws Exception {
        int max = Integer.MIN_VALUE;
        for (Tuple3<String, Long, Integer> element : elements) {
            System.out.println(context.window().toString() + ", " + element);
            max = Math.max(element.f2, max);
        }
        out.collect(Tuple3.of(key, context.window().getEnd(), max));
    }
}
