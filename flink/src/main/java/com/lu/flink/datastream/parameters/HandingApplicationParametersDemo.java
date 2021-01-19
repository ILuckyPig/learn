package com.lu.flink.datastream.parameters;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * -input xxx
 */
public class HandingApplicationParametersDemo {
    public static void main(String[] args) throws Exception {
        ParameterTool argsParameter = ParameterTool.fromArgs(args);
        String input = argsParameter.getRequired("input");
        ParameterTool propertiesParameter = ParameterTool.fromPropertiesFile(HandingApplicationParametersDemo.class
                .getResource("/log4j2-test.properties").getFile());
        String root = propertiesParameter.get("log4j.rootLogger");
        long log = propertiesParameter.getLong("log", 1L);
        ParameterTool systemProperties = ParameterTool.fromSystemProperties();
        System.out.println(input);
        System.out.println(root);
        System.out.println(log);
        System.out.println(systemProperties.getRequired("prefix"));

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.getConfig().setGlobalJobParameters(systemProperties);
        environment
                .fromElements(1, 2, 3)
                .map(new RichMapFunction<Integer, String>() {
                    private ParameterTool parameterTool;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        parameterTool = (ParameterTool) getRuntimeContext()
                                .getExecutionConfig().getGlobalJobParameters();
                    }

                    @Override
                    public String map(Integer value) throws Exception {
                        String prefix = parameterTool.getRequired("prefix");
                        return prefix + "-" + value;
                    }
                })
                .print();
        environment.execute();
    }
}
