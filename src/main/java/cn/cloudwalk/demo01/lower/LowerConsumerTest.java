package cn.cloudwalk.demo01.lower;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

public class LowerConsumerTest{

    public static void main(String[] args) throws IOException {

        InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream("kafka-consume.properties");
        Properties properties = new Properties();
        properties.load(stream);

        KafkaConsumer kafkaConsumer = new KafkaConsumer<>(properties);

    }

    private String findLeader(List<String> brokers, String partition, String topic, String port) {


        for (String broker : brokers) {
//            new simplecons<>()
        }
        return null;
    }
}
