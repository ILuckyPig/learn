kafka-topics.sh --create --topic event-time-temporal-join-demo-orders --partitions 2 --zookeeper zookeeper:2181 --replication-factor 1
kafka-topics.sh --create --topic event-time-temporal-join-demo-currency-rates --partitions 2 --zookeeper zookeeper:2181 --replication-factor 1

kafka-console-producer.sh --broker-list kafka_kafka_1:9092 --topic event-time-temporal-join-demo-orders
>1,2,Euro,2020-12-04 10:15:00
>2,1,US Dollar,2020-12-04 10:30:00
>3,50,Yen,2020-12-04 19:14:09
>4,3,Euro,2020-12-04 10:32:00
>5,5,Us Dollar,2020-12-04 10:52:00

kafka-console-producer.sh --broker-list kafka_kafka_1:9092 --topic event-time-temporal-join-demo-currency-rates
>Us Dollar,102,2020-12-04 09:00:00
>Euro,114,2020-12-04 09:00:00
>Yen,1,2020-12-04 09:00:00
>Euro,116,2020-12-04 10:45:00
>Euro,119,2020-12-04 11:15:00
>Pounds,108,2020-12-04 11:49:00