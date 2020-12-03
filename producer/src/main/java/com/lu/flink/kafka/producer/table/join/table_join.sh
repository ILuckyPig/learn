bin\windows\kafka-topics.bat --zookeeper 127.0.0.1:2181 --create --topic Orders --partitions 2 --replication-factor 1
bin\windows\kafka-topics.bat --zookeeper 127.0.0.1:2181 --create --topic RatesHistory --partitions 2 --replication-factor 1

bin\windows\kafka-console-producer.bat --broker-list 127.0.0.1:9092 --sync --topic Orders
>2020-12-04 10:15:00,2,Euro
>2020-12-04 10:30:00,1,US Dollar
>2020-12-04 19:14:09,50,Yen
>2020-12-04 10:32:00,3,Euro
>2020-12-04 10:52:00,5,Us Dollar

bin\windows\kafka-console-producer.bat --broker-list 127.0.0.1:9092 --sync --topic RatesHistory
>2020-12-04 09:00:00,Us Dollar,102
>2020-12-04 09:00:00,Euro,114
>2020-12-04 09:00:00,Yen,1
>2020-12-04 10:45:00,Euro,116
>2020-12-04 11:15:00,Euro,119
>2020-12-04 11:49:00,Pounds,108