package com.lu.flink.operators.window.incremental.reduce;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.List;

public class MyProcessWindowFunction extends ProcessWindowFunction<Tuple3<String, Long, Integer>, Tuple3<String, Long, Tuple3<String, Long, Integer>>, String, TimeWindow> {
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
