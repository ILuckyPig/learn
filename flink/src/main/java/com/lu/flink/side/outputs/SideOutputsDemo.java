package com.lu.flink.side.outputs;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SideOutputsDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> source = environment.fromElements(1, 2, 3, 4, 5, 6, 7);
        final OutputTag<String> outputTag = new OutputTag<String>("side-output"){};
        SingleOutputStreamOperator<Integer> mainStream = source.process(new ProcessFunction<Integer, Integer>() {
            @Override
            public void processElement(Integer value, Context ctx, Collector<Integer> out) throws Exception {
                out.collect(value);

                ctx.output(outputTag, "sideout-" + value);
            }
        });

        mainStream.getSideOutput(outputTag).print();
        mainStream.print();
        environment.execute();
    }
}
