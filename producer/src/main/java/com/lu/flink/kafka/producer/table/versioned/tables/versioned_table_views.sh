kafka-topics.sh --create --topic versioned-table-views-currency-rates --partitions 2 --zookeeper zookeeper:2181 --replication-factor 1
kafka-console-producer.sh --broker-list kafka_kafka_1:9092 --topic versioned-table-views-currency-rates
>{"currency": "Us Dollar", "conversion_rate": 102, "update_time": "2020-12-04 09:00:00"}
>{"currency": "Euro", "conversion_rate": 114, "update_time": "2020-12-04 09:00:00"}
>{"currency": "Euro", "conversion_rate": 116, "update_time": "2020-12-04 10:45:00"}
>{"currency": "Yen", "conversion_rate": 1, "update_time": "2020-12-04 09:00:00"}
>{"currency": "Euro", "conversion_rate": 119, "update_time": "2020-12-04 11:15:00"}
>{"currency": "Euro", "conversion_rate": 114, "update_time": "2020-12-04 09:00:00"}
>{"currency": "Yen", "conversion_rate": 100, "update_time": "2020-12-04 09:10:01"}
>{"currency": "Yen", "conversion_rate": 100, "update_time": "2020-12-04 09:01:01"}