package com.lu.flink.udf.accumulators;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

public class AccumulatorsDemo {



    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        environment
                .fromElements(Tuple2.of(1, 1), Tuple2.of(1, 2), Tuple2.of(1, 3), Tuple2.of(2, 1))
                .groupBy(0)
                .reduce(new RichReduceFunction<Tuple2<Integer, Integer>>() {
                    private final IntCounter sum = new IntCounter(0);

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        getRuntimeContext().addAccumulator("value", sum);
                    }

                    @Override
                    public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> t1,
                                                           Tuple2<Integer, Integer> t2) throws Exception {
                        t1.f1 += t2.f1;
                        sum.add(t1.f1);
                        return t1;
                    }
                })
                .printOnTaskManager("a");

        JobExecutionResult execute = environment.execute();
        System.out.println("value: " + execute.getAccumulatorResult("value").toString());
    }
}
