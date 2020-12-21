kafka-topics.sh --create --topic process-time-comment-table-join-demo-orders --bootstrap-server kafka_kafka_1:9092

kafka-console-producer.sh --bootstrap-server kafka_kafka_1:9092 --topic process-time-comment-table-join-demo-orders
{"order_id": 1, "currency": "Euro", "order_time": "2020-12-04 09:00:00"}
{"order_id": 2, "currency": "Euro", "order_time": "2020-12-04 09:00:00"}
{"order_id": 3, "currency": "Us Dollar", "order_time": "2020-12-04 09:00:00"}