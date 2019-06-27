package cn.cloudwalk.demo02;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class JConsumeSubscribeMutil {

    private static Executor executor = Executors.newFixedThreadPool(2);

    public static void main(String[] args) {

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configure());
        consumer.subscribe(Arrays.asList("ip_login"));
        boolean flag = true;
        while (flag) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            executor.execute(new KafkaConsumeThread(records));
        }

    }

    private static class KafkaConsumeThread implements Runnable {

        private ConsumerRecords<String, String> records;

        public KafkaConsumeThread(ConsumerRecords<String, String> records) {
            this.records = records;
        }

        @Override
        public void run() {

            Set<TopicPartition> partitions = records.partitions();
            for (TopicPartition partition : partitions) {
                List<ConsumerRecord<String, String>> records = this.records.records(partition);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("record.offset() == " + record.offset() + ", record.value() ==" + record.value());
                }
            }
        }
    }

    private static Properties configure() {
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
