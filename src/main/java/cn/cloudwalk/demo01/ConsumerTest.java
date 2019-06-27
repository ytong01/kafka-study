package cn.cloudwalk.demo01;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Properties;

public class ConsumerTest {

    public static void main(String[] args) throws IOException {

        InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream("kafka-consume.properties");
        Properties properties = new Properties();
        properties.load(stream);

        KafkaConsumer kafkaConsumer = new KafkaConsumer<>(properties);
        while (true) {
            try {
                kafkaConsumer.subscribe(Collections.singleton("ip_login"));
                ConsumerRecords<String, Account> consumerRecords = kafkaConsumer.poll(100);
                for (ConsumerRecord<String, Account> record : consumerRecords) {

                    System.out.println(record.value().getId());
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }
}
