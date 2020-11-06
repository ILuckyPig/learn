package com.lu.flink.kafka.producer.table;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class TableProducerDemo {
    private static final Logger log = LoggerFactory.getLogger(TableProducerDemo.class);

    private static final String[] words = {"NGA", "守望", "源氏", "寡妇", "闪光"};

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "127.0.0.1:9092");
        properties.put("acks", "0");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        String topic = "user-search-log";
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        run(producer, topic);
    }

    private static void run(KafkaProducer<String, String> producer, String topic) throws ExecutionException, InterruptedException {
        log.info("start send message to kafka");
        Random random = new Random();
        while (true) {
            // TODO
            String message = new Date(LocalDateTime.now().toInstant(ZoneOffset.of("+8")).toEpochMilli()) + ",100," + words[random.nextInt(words.length)];
            log.info("{}", message);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);
            send(producer, record);
            Thread.sleep(5 * 1000);
        }
    }

    private static void send(KafkaProducer<String, String> producer, ProducerRecord<String, String> record) throws ExecutionException, InterruptedException {
        Future<RecordMetadata> future = producer.send(record);
        future.get();
    }
}
