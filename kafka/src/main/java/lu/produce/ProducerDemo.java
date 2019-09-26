package lu.produce;

import com.lu.utils.YamlUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ProducerDemo {

    private Map<Integer, String> userMap = YamlUtils.getUserMap();

    private String getUser() {
        int count = userMap.size();
        Random random = new Random();
        int index = random.nextInt(count);
        int uid = 100000 + index;
        return userMap.get(uid);
    }

    private String getContext() {
        return UUID.randomUUID().toString();
    }

    private String produceMessage() {
        return LocalDateTime.now() + " " + getUser() + " " + getContext();
    }

    private void execute(String topic, Properties properties) throws InterruptedException, ExecutionException {
        System.out.println("start");
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        // date uid,gender,city context
        while (true) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, produceMessage());
            Thread.sleep(1000);
            Future<RecordMetadata> send = producer.send(record);
            send.get();
        }
    }

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "127.0.0.1:9092");
        properties.put("acks", "0");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        String topic = args[0];
        ProducerDemo producerDemo = new ProducerDemo();
        producerDemo.execute(topic, properties);
    }
}
