kafka-topics.sh --create --topic fs-state-backend --partitions 2 --zookeeper zookeeper:2181 --replication-factor 1
kafka-console-producer.sh --broker-list kafka_kafka_1:9092 --topic fs-state-backend
kafka-consumer-groups.sh --bootstrap-server kafka_kafka_1:9092 --topic fs-state-backend --group kafka-state-demo-group --reset-offsets --to-earliest --execute
kafka-consumer-groups.sh --bootstrap-server kafka_kafka_1:9092 --describe --group kafka-state-demo-group
{"id": 1}
{"id": 1}
{"id": 1}
{"id": 1}
{"id": 1}
{"id": 1}
{"id": 1}
{"id": 1}
{"id": 1}
{"id": 1}