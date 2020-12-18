package com.lu.flink.table.versioned.tables;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * >{"currency": "Us Dollar", "conversion_rate": 102, "update_time": "2020-12-04 09:00:00"}
 * >{"currency": "Euro", "conversion_rate": 114, "update_time": "2020-12-04 09:00:00"}
 * >{"currency": "Euro", "conversion_rate": 116, "update_time": "2020-12-04 10:45:00"}
 * >{"currency": "Yen", "conversion_rate": 1, "update_time": "2020-12-04 09:00:00"}
 * >{"currency": "Euro", "conversion_rate": 119, "update_time": "2020-12-04 11:15:00"}
 * >{"currency": "Euro", "conversion_rate": 114, "update_time": "2020-12-04 09:00:00"}
 * >{"currency": "Yen", "conversion_rate": 100, "update_time": "2020-12-04 09:10:01"}
 * >{"currency": "Yen", "conversion_rate": 100, "update_time": "2020-12-04 09:01:01"}
 *
 * 1> +I(Us Dollar,102.00,2020-12-04T09:00)
 * 1> +I(Euro,114.00,2020-12-04T09:00)
 * 1> -U(Euro,114.00,2020-12-04T09:00)
 * 1> +U(Euro,116.00,2020-12-04T10:45)
 * 1> +I(Yen,1.00,2020-12-04T09:00)
 * 1> -U(Euro,116.00,2020-12-04T10:45)
 * 1> +U(Euro,119.00,2020-12-04T11:15)
 * 1> -U(Yen,1.00,2020-12-04T09:00)
 * 1> +U(Yen,100.00,2020-12-04T09:10:01)
 *
 */
public class VersionedTableViews {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(2);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment, settings);
        tableEnvironment.executeSql(
                "CREATE TABLE currency_rates (" +
                        "    currency STRING," +
                        "    conversion_rate DECIMAL(32, 2)," +
                        "    update_time TIMESTAMP(3)," +
                        "    WATERMARK FOR update_time AS update_time" +
                        ") WITH (" +
                        "   'connector' = 'kafka'," +
                        "   'topic' = 'versioned-table-views-currency-rates'," +
                        "   'properties.bootstrap.servers' = 'localhost:9095'," +
                        "   'properties.group.id' = 'kafka-table-demo-group'," +
                        "   'format' = 'json'," +
                        "   'scan.startup.mode' = 'earliest-offset'" +
                        ")"
        );
        tableEnvironment.executeSql(
                "CREATE VIEW versioned_rates AS" +
                        "   SELECT currency, conversion_rate, update_time" +
                        "   FROM (" +
                        "       SELECT *," +
                        "       ROW_NUMBER() OVER (PARTITION BY currency ORDER BY update_time DESC) AS rownum" +
                        "       FROM currency_rates)" +
                        "   WHERE rownum = 1"
        );
        tableEnvironment.executeSql("CREATE TABLE print_table WITH ('connector'='print') LIKE currency_rates (EXCLUDING ALL)");
        tableEnvironment.executeSql("INSERT INTO print_table SELECT * FROM versioned_rates");
        environment.execute();
    }
}
