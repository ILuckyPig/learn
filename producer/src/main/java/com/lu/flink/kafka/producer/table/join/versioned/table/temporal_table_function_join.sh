kafka-topics.sh --create --topic temporal-table-function-join-demo-rates-history --bootstrap-server kafka_kafka_1:9092 --partitions 1
kafka-topics.sh --create --topic temporal-table-function-join-demo-orders --bootstrap-server kafka_kafka_1:9092 --partitions 1

kafka-console-producer.sh --topic temporal-table-function-join-demo-rates-history --bootstrap-server kafka_kafka_1:9092
Us Dollar,110,2020-12-04 09:00:00
Euro,110,2020-12-04 09:10:00
Euro,110,2020-12-04 09:20:00

kafka-console-producer.sh --topic temporal-table-function-join-demo-orders --bootstrap-server kafka_kafka_1:9092
{"id": 1, "currency": "Euro", "amount": 2, "order_time": "2020-12-04 01:00:00"}
{"id": 2, "currency": "Euro", "amount": 2, "order_time": "2020-12-04 01:00:00"}
{"id": 3, "currency": "Euro", "amount": 2, "order_time": "2020-12-04 01:10:00"}
{"id": 4, "currency": "Us Dollar", "amount": 10, "order_time": "2020-12-04 01:00:00"}
{"id": 5, "currency": "Euro", "amount": 2, "order_time": "2020-12-04 01:00:10"}
{"id": 6, "currency": "Euro", "amount": 2, "order_time": "2020-12-04 01:20:00"}
