kafka-topics.sh --create --topic versioned-table-currency-rates --partitions 2 --zookeeper zookeeper:2181 --replication-factor 1
kafka-console-producer.sh --broker-list kafka_kafka_1:9092 --topic versioned-table-currency-rates
>{ "op": "c", "before": null, "after": {"currency": "Us Dollar", "conversion_rate": 102, "update_time": "2020-12-04 09:00:00"}, "source": { "version": "1.0.3.Final", "connector": "mysql", "name": "mysql-test-1", "query": null, "ts_ms": 1607043600000, "snapshot": "false", "db": "test", "table": "currency_rates", "server_id": 0, "gtid": null, "file": "mysql-bin.000007", "pos": 354, "row": 0, "thread": 4 }, "ts_ms": 1607043600000 }
>{ "op": "c", "before": null, "after": {"currency": "Euro", "conversion_rate": 114, "update_time": "2020-12-04 09:00:00"}, "source": { "version": "1.0.3.Final", "connector": "mysql", "name": "mysql-test-1", "query": null, "ts_ms": 1607043600000, "snapshot": "false", "db": "test", "table": "currency_rates", "server_id": 0, "gtid": null, "file": "mysql-bin.000007", "pos": 354, "row": 0, "thread": 4 }, "ts_ms": 1607043600000 }
>{ "op": "u", "before": {"currency": "Euro", "conversion_rate": 114, "update_time": "2020-12-04 09:00:00"}, "after": {"currency": "Euro", "conversion_rate": 116, "update_time": "2020-12-04 10:45:00"}, "source": { "version": "1.0.3.Final", "connector": "mysql", "name": "mysql-test-1", "query": null, "ts_ms": 1607049900000, "snapshot": "false", "db": "test", "table": "currency_rates", "server_id": 0, "gtid": null, "file": "mysql-bin.000007", "pos": 354, "row": 0, "thread": 4 }, "ts_ms": 1607049900000 }
>{ "op": "c", "before": null, "after": {"currency": "Yen", "conversion_rate": 1, "update_time": "2020-12-04 09:00:00"}, "source": { "version": "1.0.3.Final", "connector": "mysql", "name": "mysql-test-1", "query": null, "ts_ms": 1607043600000, "snapshot": "false", "db": "test", "table": "currency_rates", "server_id": 0, "gtid": null, "file": "mysql-bin.000007", "pos": 354, "row": 0, "thread": 4 }, "ts_ms": 1607043600000 }
>{ "op": "u", "before": {"currency": "Euro", "conversion_rate": 116, "update_time": "2020-12-04 10:45:00"}, "after": {"currency": "Euro", "conversion_rate": 119, "update_time": "2020-12-04 11:15:00"}, "source": { "version": "1.0.3.Final", "connector": "mysql", "name": "mysql-test-1", "query": null, "ts_ms": 1607051700000, "snapshot": "false", "db": "test", "table": "currency_rates", "server_id": 0, "gtid": null, "file": "mysql-bin.000007", "pos": 354, "row": 0, "thread": 4 }, "ts_ms": 1607051700000 }
>{ "op": "u", "before": {"currency": "Euro", "conversion_rate": 114, "update_time": "2020-12-04 09:00:00"}, "after": {"currency": "Euro", "conversion_rate": 114, "update_time": "2020-12-04 09:00:00"}, "source": { "version": "1.0.3.Final", "connector": "mysql", "name": "mysql-test-1", "query": null, "ts_ms": 1607043600000, "snapshot": "false", "db": "test", "table": "currency_rates", "server_id": 0, "gtid": null, "file": "mysql-bin.000007", "pos": 354, "row": 0, "thread": 4 }, "ts_ms": 1607043600000 }
>{ "op": "u", "before": {"currency": "Yen", "conversion_rate": 100, "update_time": "2020-12-04 09:01:01"}, "after": {"currency": "Yen", "conversion_rate": 100, "update_time": "2020-12-04 09:10:01"}, "source": { "version": "1.0.3.Final", "connector": "mysql", "name": "mysql-test-1", "query": null, "ts_ms": 1607044201000, "snapshot": "false", "db": "test", "table": "currency_rates", "server_id": 0, "gtid": null, "file": "mysql-bin.000007", "pos": 354, "row": 0, "thread": 4 }, "ts_ms": 1607044201000 }
>{ "op": "u", "before": {"currency": "Yen", "conversion_rate": 1, "update_time": "2020-12-04 09:00:00"}, "after": {"currency": "Yen", "conversion_rate": 100, "update_time": "2020-12-04 09:01:01"}, "source": { "version": "1.0.3.Final", "connector": "mysql", "name": "mysql-test-1", "query": null, "ts_ms": 1607043600000, "snapshot": "false", "db": "test", "table": "currency_rates", "server_id": 0, "gtid": null, "file": "mysql-bin.000007", "pos": 354, "row": 0, "thread": 4 }, "ts_ms": 1607043600000 }