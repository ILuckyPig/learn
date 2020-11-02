package com.lu.flink.kafka.producer.eventtime;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * 配合Flink周期性watermark
 */
public class EventDemoProducer {
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "127.0.0.1:9092");
        properties.put("acks", "0");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        String topic = "demo";
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        while (true) {
            String message = System.currentTimeMillis() + ",1";
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);
            Thread.sleep(1000);
            Future<RecordMetadata> send = producer.send(record);
            send.get();
        }
    }
}
