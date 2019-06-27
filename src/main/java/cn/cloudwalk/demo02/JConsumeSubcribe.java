package cn.cloudwalk.demo02;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class JConsumeSubcribe extends Thread {

    public static void main(String[] args) {

        new JConsumeSubcribe().start();
    }

    @Override
    public void run() {

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(configure());
        kafkaConsumer.subscribe(Arrays.asList("ip_login"));

        boolean flag = true;
        while (flag) {

            ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("record.offset() == " + record.offset() + ", record.value() ==" + record.value());
            }
        }
    }

    private Properties configure() {
        Properties properties = new Properties();
        //注意
        //需要在server.properties文件配置listeners=PLAINTEXT://10.129.0.21:9092
        properties.put("bootstrap.servers", "10.129.0.21:9092");
        properties.put("group.id", "test");
        //自动提交偏移量
        properties.put("enable.auto.commit", true);
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return properties;
    }
}
