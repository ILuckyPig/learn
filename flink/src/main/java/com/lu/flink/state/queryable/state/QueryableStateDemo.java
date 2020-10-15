package com.lu.flink.state.queryable.state;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.queryablestate.client.QueryableStateClient;

import java.util.concurrent.CompletableFuture;

/**
 * based on run {@link QueryableStateSourceDemo}
 */
public class QueryableStateDemo {
    public static void main(String[] args) throws Exception {
        JobID jobId = JobID.fromHexString(args[0]);
        QueryableStateClient client = new QueryableStateClient("localhost", 9069);

        ValueStateDescriptor<Tuple2<Integer, Integer>> descriptor =
                new ValueStateDescriptor<>("average", Types.TUPLE(Types.INT, Types.INT));

        CompletableFuture<ValueState<Tuple2<Integer, Integer>>> future = client.getKvState(jobId,
                "query-name", 2, Types.INT, descriptor);

        ValueState<Tuple2<Integer, Integer>> tuple2ValueState = future.get();
        System.out.println(tuple2ValueState.value());
    }
}
