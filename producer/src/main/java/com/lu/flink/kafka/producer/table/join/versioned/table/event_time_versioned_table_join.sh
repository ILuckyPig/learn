kafka-topics.sh --create --topic event-time-versioned-table-join-demo-currency-rates --bootstrap-server kafka_kafka_1:9092
kafka-topics.sh --create --topic event-time-versioned-table-join-demo-orders --bootstrap-server kafka_kafka_1:9092

kafka-console-producer.sh --bootstrap-server kafka_kafka_1:9092 --topic event-time-versioned-table-join-demo-orders
{"order_id": 1, "currency": "Euro", "order_time": "2020-12-04 09:00:00"}
{"order_id": 2, "currency": "Euro", "order_time": "2020-12-04 09:00:00"}
{"order_id": 3, "currency": "Euro", "order_time": "2020-12-04 09:00:10"}
{"order_id": 4, "currency": "Us Dollar", "order_time": "2020-12-04 09:00:00"}
{"order_id": 5, "currency": "Euro", "order_time": "2020-12-04 09:10:00"}
{"order_id": 6, "currency": "Euro", "order_time": "2020-12-04 09:45:00"}
{"order_id": 7, "currency": "Euro", "order_time": "2020-12-04 10:45:00"}
{"order_id": 8, "currency": "Yen", "order_time": "2020-12-04 11:01:10"}

kafka-console-producer.sh --bootstrap-server kafka_kafka_1:9092 --topic event-time-versioned-table-join-demo-currency-rates
{ "op": "c", "before": null, "after": {"currency": "Us Dollar", "conversion_rate": 102, "update_time": "2020-12-04 09:00:00"}, "source": { "version": "1.0.3.Final", "connector": "mysql", "name": "mysql-test-1", "query": null, "ts_ms": 1607043600000, "snapshot": "false", "db": "test", "table": "currency_rates", "server_id": 0, "gtid": null, "file": "mysql-bin.000007", "pos": 354, "row": 0, "thread": 4 }, "ts_ms": 1607043600000 }
{ "op": "c", "before": null, "after": {"currency": "Euro", "conversion_rate": 114, "update_time": "2020-12-04 09:10:00"}, "source": { "version": "1.0.3.Final", "connector": "mysql", "name": "mysql-test-1", "query": null, "ts_ms": 1607044200000, "snapshot": "false", "db": "test", "table": "currency_rates", "server_id": 0, "gtid": null, "file": "mysql-bin.000007", "pos": 354, "row": 0, "thread": 4 }, "ts_ms": 1607044200000 }
{ "op": "u", "before": {"currency": "Euro", "conversion_rate": 114, "update_time": "2020-12-04 09:10:00"}, "after": {"currency": "Euro", "conversion_rate": 116, "update_time": "2020-12-04 10:45:00"}, "source": { "version": "1.0.3.Final", "connector": "mysql", "name": "mysql-test-1", "query": null, "ts_ms": 1607049900000, "snapshot": "false", "db": "test", "table": "currency_rates", "server_id": 0, "gtid": null, "file": "mysql-bin.000007", "pos": 354, "row": 0, "thread": 4 }, "ts_ms": 1607049900000 }
{ "op": "c", "before": null, "after": {"currency": "Yen", "conversion_rate": 1, "update_time": "2020-12-04 11:01:10"}, "source": { "version": "1.0.3.Final", "connector": "mysql", "name": "mysql-test-1", "query": null, "ts_ms": 1607050870000, "snapshot": "false", "db": "test", "table": "currency_rates", "server_id": 0, "gtid": null, "file": "mysql-bin.000007", "pos": 354, "row": 0, "thread": 4 }, "ts_ms": 1607050870000 }