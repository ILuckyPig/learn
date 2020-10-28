package com.lu.flink.dataset.semantic.annotations;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class ForwardedFieldsAnnotationDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<Tuple2<Integer, Integer>> source = environment.fromElements(Tuple2.of(1, 1), Tuple2.of(1, 2));
        source.map(tuple2 -> Tuple3.of("foo", tuple2.f1 / 2, tuple2.f0))
                .returns(Types.TUPLE(Types.STRING, Types.INT, Types.INT))
                .withForwardedFields("f0->f2")
                .printOnTaskManager("demo1");

        source.map(new NonForwardMap()).printOnTaskManager("demo2");


        environment.execute();
    }

    @FunctionAnnotation.NonForwardedFields("f1")
    static
    class NonForwardMap implements MapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {
        @Override
        public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> tuple2) throws Exception {
            return Tuple2.of(tuple2.f0, tuple2.f1 / 2);
        }
    }
}
