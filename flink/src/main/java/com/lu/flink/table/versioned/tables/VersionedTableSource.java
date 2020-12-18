package com.lu.flink.table.versioned.tables;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * >{ "op": "c", "before": null, "after": {"currency": "Us Dollar", "conversion_rate": 102, "update_time": "2020-12-04 09:00:00"}, "source": { "version": "1.0.3.Final", "connector": "mysql", "name": "mysql-test-1", "query": null, "ts_ms": 1607043600000, "snapshot": "false", "db": "test", "table": "currency_rates", "server_id": 0, "gtid": null, "file": "mysql-bin.000007", "pos": 354, "row": 0, "thread": 4 }, "ts_ms": 1607043600000 }
 * >{ "op": "c", "before": null, "after": {"currency": "Euro", "conversion_rate": 114, "update_time": "2020-12-04 09:00:00"}, "source": { "version": "1.0.3.Final", "connector": "mysql", "name": "mysql-test-1", "query": null, "ts_ms": 1607043600000, "snapshot": "false", "db": "test", "table": "currency_rates", "server_id": 0, "gtid": null, "file": "mysql-bin.000007", "pos": 354, "row": 0, "thread": 4 }, "ts_ms": 1607043600000 }
 * >{ "op": "u", "before": {"currency": "Euro", "conversion_rate": 114, "update_time": "2020-12-04 09:00:00"}, "after": {"currency": "Euro", "conversion_rate": 116, "update_time": "2020-12-04 10:45:00"}, "source": { "version": "1.0.3.Final", "connector": "mysql", "name": "mysql-test-1", "query": null, "ts_ms": 1607049900000, "snapshot": "false", "db": "test", "table": "currency_rates", "server_id": 0, "gtid": null, "file": "mysql-bin.000007", "pos": 354, "row": 0, "thread": 4 }, "ts_ms": 1607049900000 }
 * >{ "op": "c", "before": null, "after": {"currency": "Yen", "conversion_rate": 1, "update_time": "2020-12-04 09:00:00"}, "source": { "version": "1.0.3.Final", "connector": "mysql", "name": "mysql-test-1", "query": null, "ts_ms": 1607043600000, "snapshot": "false", "db": "test", "table": "currency_rates", "server_id": 0, "gtid": null, "file": "mysql-bin.000007", "pos": 354, "row": 0, "thread": 4 }, "ts_ms": 1607043600000 }
 * >{ "op": "u", "before": {"currency": "Euro", "conversion_rate": 116, "update_time": "2020-12-04 10:45:00"}, "after": {"currency": "Euro", "conversion_rate": 119, "update_time": "2020-12-04 11:15:00"}, "source": { "version": "1.0.3.Final", "connector": "mysql", "name": "mysql-test-1", "query": null, "ts_ms": 1607051700000, "snapshot": "false", "db": "test", "table": "currency_rates", "server_id": 0, "gtid": null, "file": "mysql-bin.000007", "pos": 354, "row": 0, "thread": 4 }, "ts_ms": 1607051700000 }
 * >{ "op": "u", "before": {"currency": "Euro", "conversion_rate": 114, "update_time": "2020-12-04 09:00:00"}, "after": {"currency": "Euro", "conversion_rate": 114, "update_time": "2020-12-04 09:00:00"}, "source": { "version": "1.0.3.Final", "connector": "mysql", "name": "mysql-test-1", "query": null, "ts_ms": 1607043600000, "snapshot": "false", "db": "test", "table": "currency_rates", "server_id": 0, "gtid": null, "file": "mysql-bin.000007", "pos": 354, "row": 0, "thread": 4 }, "ts_ms": 1607043600000 }
 * >{ "op": "u", "before": {"currency": "Yen", "conversion_rate": 100, "update_time": "2020-12-04 09:01:01"}, "after": {"currency": "Yen", "conversion_rate": 100, "update_time": "2020-12-04 09:10:01"}, "source": { "version": "1.0.3.Final", "connector": "mysql", "name": "mysql-test-1", "query": null, "ts_ms": 1607044201000, "snapshot": "false", "db": "test", "table": "currency_rates", "server_id": 0, "gtid": null, "file": "mysql-bin.000007", "pos": 354, "row": 0, "thread": 4 }, "ts_ms": 1607044201000 }
 * >{ "op": "u", "before": {"currency": "Yen", "conversion_rate": 1, "update_time": "2020-12-04 09:00:00"}, "after": {"currency": "Yen", "conversion_rate": 100, "update_time": "2020-12-04 09:01:01"}, "source": { "version": "1.0.3.Final", "connector": "mysql", "name": "mysql-test-1", "query": null, "ts_ms": 1607043600000, "snapshot": "false", "db": "test", "table": "currency_rates", "server_id": 0, "gtid": null, "file": "mysql-bin.000007", "pos": 354, "row": 0, "thread": 4 }, "ts_ms": 1607043600000 }
 *
 * 2> +I(Euro,114.00,2020-12-04T09:00)
 * 1> +I(Us Dollar,102.00,2020-12-04T09:00)
 * 2> +I(Yen,1.00,2020-12-04T09:00)
 * 1> -U(Euro,114.00,2020-12-04T10:45)
 * 1> +U(Euro,116.00,2020-12-04T10:45)
 * 2> -U(Euro,114.00,2020-12-04T09:00)
 * 2> +U(Euro,114.00,2020-12-04T09:00)
 * 1> -U(Euro,116.00,2020-12-04T11:15)
 * 1> +U(Euro,119.00,2020-12-04T11:15)
 * 2> -U(Yen,1.00,2020-12-04T09:00)
 * 2> +U(Yen,100.00,2020-12-04T09:00)
 * 1> -U(Yen,100.00,2020-12-04T09:10:01)
 * 1> +U(Yen,100.00,2020-12-04T09:10:01)
 */
public class VersionedTableSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(2);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment, settings);

        tableEnvironment.executeSql(
                "CREATE TABLE currency_rates (" +
                        "   currency STRING," +
                        "   conversion_rate DECIMAL(32, 2)," +
                        "   update_time TIMESTAMP(3) METADATA FROM 'value.source.timestamp' VIRTUAL," +
                        "   PRIMARY KEY (currency) NOT ENFORCED," +
                        "   WATERMARK FOR update_time AS update_time" +
                        ") WITH (" +
                        "   'connector' = 'kafka'," +
                        "   'topic' = 'versioned-table-currency-rates'," +
                        "   'properties.bootstrap.servers' = 'localhost:9095'," +
                        "   'properties.group.id' = 'kafka-table-demo-group'," +
                        "   'format' = 'debezium-json'," +
                        "   'scan.startup.mode' = 'earliest-offset'" +
                        ")"
        );
        tableEnvironment.executeSql(
                "CREATE TABLE print_table (" +
                        "   currency STRING," +
                        "   conversion_rate DECIMAL(32, 2)," +
                        "   update_time TIMESTAMP(3)" +
                        ") WITH ('connector'='print')"
        );
        tableEnvironment.executeSql("INSERT INTO print_table SELECT * FROM currency_rates");
        environment.execute();
    }
}
