kafka-topics.sh --create --topic savepoint-demo --partitions 2 --zookeeper zookeeper:2181 --replication-factor 1
kafka-console-producer.sh --broker-list kafka_kafka_1:9092 --topic savepoint-demo
kafka-consumer-groups.sh --bootstrap-server kafka_kafka_1:9092 --topic savepoint-demo --group save-point-demo-group --reset-offsets --to-earliest --execute
kafka-consumer-groups.sh --bootstrap-server kafka_kafka_1:9092 --describe --group save-point-demo-group
{"id": 1}
{"id": 1}
{"id": 1}
{"id": 1}
# savepoint
{"id": 1}
{"id": 1}
{"id": 1}
{"id": 1}
{"id": 1}
{"id": 1}